#include "raft/raft_node.hpp"

#include <algorithm>
#include <filesystem>
#include <utility>

namespace {

uint64_t SegmentSizeBytes(const StorageConfig& storage) {
    return static_cast<uint64_t>(storage.wal_segment_size_mb) * 1024ULL * 1024ULL;
}

} // namespace

RaftNode::RaftNode(Config config,
                   RaftNodeOptions options,
                   std::unique_ptr<WAL> wal,
                   RaftMeta meta,
                   std::unique_ptr<TopologyManager> topology,
                   std::unique_ptr<LeaseManager> lease)
    : config_(std::move(config)),
      self_id_(config_.cluster.node_id),
      self_addr_(std::move(options.self_addr)),
      current_term_(meta.current_term),
      wal_(std::move(wal)),
      meta_(std::move(meta)),
      topology_(std::move(topology)),
      lease_(std::move(lease)),
      vector_index_(std::move(options.vector_index)),
      dedup_table_(std::move(options.dedup_table)),
      raft_client_(std::move(options.raft_client)) {
    voted_for_ = meta_.voted_for;
}

Result<std::shared_ptr<RaftNode>> RaftNode::Create(const Config& config, RaftNodeOptions options) {
    if (config.cluster.node_id.empty()) {
        return Result<std::shared_ptr<RaftNode>>::Err("RaftNode 创建失败: cluster.node_id 不能为空");
    }

    auto wal = WAL::Open(config.storage.raft_log_dir, 1, SegmentSizeBytes(config.storage));
    if (!wal) {
        return Result<std::shared_ptr<RaftNode>>::Err(wal.error);
    }

    auto meta = RaftMeta::Load((std::filesystem::path(config.storage.raft_log_dir) / "meta.bin").string());
    if (!meta) {
        return Result<std::shared_ptr<RaftNode>>::Err(meta.error);
    }

    auto topology = std::make_unique<TopologyManager>(config.cluster.node_id, options.self_addr);
    auto lease = std::make_unique<LeaseManager>();
    if (!options.dedup_table) {
        options.dedup_table = std::make_shared<DedupTable>(config.client.dedup_window_size);
    }
    if (!options.raft_client) {
        options.raft_client = std::make_shared<RaftClient>();
    }

    auto node = std::shared_ptr<RaftNode>(new RaftNode(config, std::move(options), std::move(*wal),
                                                       std::move(*meta), std::move(topology),
                                                       std::move(lease)));
    node->RegisterConfiguredPeers();
    return Result<std::shared_ptr<RaftNode>>::Ok(std::move(node));
}

Result<uint64_t> RaftNode::Propose(const LogEntry& entry) {
    std::lock_guard lock(state_mutex_);
    if (state_.load(std::memory_order_acquire) != RaftState::kLeader) {
        return Result<uint64_t>::Err("当前节点不是 Leader，无法执行 Propose");
    }
    return AppendEntryLocked(entry);
}

Result<uint64_t> RaftNode::LeaseRead() {
    if (!lease_->IsValid()) {
        return Result<uint64_t>::Err("租约无效，当前不能执行 LeaseRead");
    }

    const uint64_t commit_index = commit_index_.load(std::memory_order_acquire);
    const uint64_t applied_index = applied_index_.load(std::memory_order_acquire);
    if (applied_index < commit_index) {
        return Result<uint64_t>::Err("状态机尚未追上 commit_index");
    }
    return Result<uint64_t>::Ok(commit_index);
}

Result<raftvdb::proto::AppendEntriesResponse> RaftNode::HandleAppendEntries(
    const raftvdb::proto::AppendEntriesRequest& request) {
    raftvdb::proto::AppendEntriesResponse response;
    response.set_node_id(self_id_);

    uint64_t current_term = CurrentTerm();
    if (request.term() < current_term) {
        response.set_term(current_term);
        response.set_success(false);
        response.set_conflict_index(wal_->LastIndex() + 1);
        return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
    }

    auto become_follower = BecomeFollower(request.term(), request.leader_id());
    if (!become_follower) {
        return Result<raftvdb::proto::AppendEntriesResponse>::Err(become_follower.error);
    }

    if (!request.topology().role().empty()) {
        auto apply_topology =
            topology_->FromProto(self_id_, request.topology(), self_addr_);
        if (!apply_topology) {
            return Result<raftvdb::proto::AppendEntriesResponse>::Err(apply_topology.error);
        }
    }

    response.set_term(CurrentTerm());
    // T-18 仅实现状态切换骨架，真正的 prevLog 校验和日志写入将在 T-21 补齐。
    // 因此这里仅在空 entries 场景下返回“当前状态可接受 Leader 请求”的保守成功。
    if (request.entries_size() == 0) {
        response.set_success(true);
        response.set_match_index(wal_->LastIndex());
    } else {
        response.set_success(false);
        response.set_conflict_index(wal_->LastIndex() + 1);
    }
    return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::RequestVoteResponse> RaftNode::HandleRequestVote(
    const raftvdb::proto::RequestVoteRequest& request) {
    raftvdb::proto::RequestVoteResponse response;

    uint64_t current_term = CurrentTerm();
    if (request.term() < current_term) {
        response.set_term(current_term);
        response.set_vote_granted(false);
        return Result<raftvdb::proto::RequestVoteResponse>::Ok(std::move(response));
    }

    if (request.term() > current_term) {
        auto become_follower = BecomeFollower(request.term());
        if (!become_follower) {
            return Result<raftvdb::proto::RequestVoteResponse>::Err(become_follower.error);
        }
    }

    response.set_term(CurrentTerm());
    // T-18 只把“更高 term 触发降级”这层骨架接好；
    // 投票授权和日志新旧比较将在 T-19 的正式选举逻辑中实现。
    response.set_vote_granted(false);
    return Result<raftvdb::proto::RequestVoteResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::HeartbeatResponse> RaftNode::HandleHeartbeat(
    const raftvdb::proto::HeartbeatRequest& request) {
    raftvdb::proto::HeartbeatResponse response;
    response.set_node_id(self_id_);

    uint64_t current_term = CurrentTerm();
    if (request.term() < current_term) {
        response.set_term(current_term);
        response.set_success(false);
        return Result<raftvdb::proto::HeartbeatResponse>::Ok(std::move(response));
    }

    auto become_follower = BecomeFollower(request.term(), request.leader_id());
    if (!become_follower) {
        return Result<raftvdb::proto::HeartbeatResponse>::Err(become_follower.error);
    }

    if (!request.topology().role().empty()) {
        auto apply_topology =
            topology_->FromProto(self_id_, request.topology(), self_addr_);
        if (!apply_topology) {
            return Result<raftvdb::proto::HeartbeatResponse>::Err(apply_topology.error);
        }
    }

    // 这里先保守推进本地 commit_index 视图，真正的心跳处理细节在 T-20 补齐。
    const uint64_t bounded_commit = std::min<uint64_t>(request.commit_index(), wal_->LastIndex());
    commit_index_.store(bounded_commit, std::memory_order_release);

    response.set_term(CurrentTerm());
    response.set_success(true);
    return Result<raftvdb::proto::HeartbeatResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::InstallSnapshotResponse> RaftNode::HandleInstallSnapshot(
    const std::vector<raftvdb::proto::SnapshotChunk>& chunks) {
    raftvdb::proto::InstallSnapshotResponse response;
    response.set_term(CurrentTerm());

    if (chunks.empty()) {
        response.set_success(false);
        return Result<raftvdb::proto::InstallSnapshotResponse>::Ok(std::move(response));
    }

    const uint64_t incoming_term = chunks.back().raft_term();
    if (incoming_term > CurrentTerm()) {
        auto become_follower = BecomeFollower(incoming_term);
        if (!become_follower) {
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(become_follower.error);
        }
    }

    response.set_term(CurrentTerm());
    // T-30 会在这里真正接收快照流、重建索引并恢复 DedupTable。
    response.set_success(false);
    return Result<raftvdb::proto::InstallSnapshotResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::LeaderInfo> RaftNode::GetLeaderInfo() const {
    raftvdb::proto::LeaderInfo info;
    info.set_term(CurrentTerm());
    info.set_leader_id(LeaderId());
    info.set_leader_addr(LeaderAddr());
    return Result<raftvdb::proto::LeaderInfo>::Ok(std::move(info));
}

bool RaftNode::IsLeader() const {
    return state_.load(std::memory_order_acquire) == RaftState::kLeader;
}

RaftState RaftNode::State() const {
    return state_.load(std::memory_order_acquire);
}

uint64_t RaftNode::CurrentTerm() const {
    return current_term_.load(std::memory_order_acquire);
}

uint64_t RaftNode::CommitIndex() const {
    return commit_index_.load(std::memory_order_acquire);
}

uint64_t RaftNode::AppliedIndex() const {
    return applied_index_.load(std::memory_order_acquire);
}

std::string RaftNode::LeaderId() const {
    std::lock_guard lock(state_mutex_);
    return leader_id_;
}

std::string RaftNode::LeaderAddr() const {
    std::string leader_id;
    {
        std::lock_guard lock(state_mutex_);
        leader_id = leader_id_;
    }

    if (leader_id.empty()) {
        return {};
    }
    if (leader_id == self_id_) {
        return self_addr_.empty() ? self_id_ : self_addr_;
    }

    auto leader = topology_->GetNode(leader_id);
    if (leader && !leader->addr.empty()) {
        return leader->addr;
    }
    return leader_id;
}

Result<void> RaftNode::BecomeFollower(uint64_t term, const std::string& leader_id) {
    std::lock_guard lock(state_mutex_);
    const uint64_t current_term = current_term_.load(std::memory_order_acquire);
    if (term < current_term) {
        return Result<void>::Err("BecomeFollower 失败: term 不能回退");
    }

    auto persist = PersistMetaLocked(term, "");
    if (!persist) {
        return persist;
    }

    current_term_.store(term, std::memory_order_release);
    voted_for_.clear();
    leader_id_ = leader_id;
    state_.store(RaftState::kFollower, std::memory_order_release);
    lease_->Invalidate();

    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        peer_progress_.clear();
    }

    if (!leader_id.empty()) {
        topology_->SetLeader(leader_id);
    }

    return Result<void>::Ok();
}

Result<void> RaftNode::BecomeCandidate() {
    std::lock_guard lock(state_mutex_);
    const uint64_t next_term = current_term_.load(std::memory_order_acquire) + 1;

    auto persist = PersistMetaLocked(next_term, self_id_);
    if (!persist) {
        return persist;
    }

    current_term_.store(next_term, std::memory_order_release);
    voted_for_ = self_id_;
    leader_id_.clear();
    state_.store(RaftState::kCandidate, std::memory_order_release);
    lease_->Invalidate();

    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        peer_progress_.clear();
    }

    return Result<void>::Ok();
}

Result<void> RaftNode::BecomeLeader() {
    std::lock_guard lock(state_mutex_);
    uint64_t term = current_term_.load(std::memory_order_acquire);
    if (term == 0) {
        // 单节点或测试直切 Leader 时，仍需把任期推进到一个有效正值，
        // 避免后续追加的 kNoop 落在 term=0 这种不自然状态。
        term = 1;
        auto persist = PersistMetaLocked(term, self_id_);
        if (!persist) {
            return persist;
        }
        current_term_.store(term, std::memory_order_release);
        voted_for_ = self_id_;
    }

    leader_id_ = self_id_;
    topology_->SetLeader(self_id_, self_addr_);

    auto noop_index = AppendNoopEntryLocked();
    if (!noop_index) {
        return Result<void>::Err(noop_index.error);
    }

    ResetPeerProgressLocked(*noop_index + 1);

    auto rebalance = topology_->Rebalance(config_.cluster.peers);
    if (!rebalance) {
        return rebalance;
    }

    state_.store(RaftState::kLeader, std::memory_order_release);
    lease_->Invalidate();
    return Result<void>::Ok();
}

Result<uint64_t> RaftNode::AppendEntryLocked(const LogEntry& entry) {
    LogEntry next = entry;
    next.index = wal_->LastIndex() + 1;
    next.term = current_term_.load(std::memory_order_acquire);

    auto append = wal_->Append(next);
    if (!append) {
        return Result<uint64_t>::Err(append.error);
    }
    auto flush = wal_->Flush();
    if (!flush) {
        return Result<uint64_t>::Err(flush.error);
    }
    return Result<uint64_t>::Ok(next.index);
}

Result<uint64_t> RaftNode::AppendNoopEntryLocked() {
    LogEntry noop;
    noop.type = EntryType::kNoop;
    noop.cmd_type = CmdType::kUpsert;
    noop.payload.clear();
    return AppendEntryLocked(noop);
}

Result<void> RaftNode::PersistMetaLocked(uint64_t term, const std::string& voted_for) {
    RaftMeta next_meta;
    next_meta.current_term = term;
    next_meta.voted_for = voted_for;
    auto save = next_meta.Save(MetaPath());
    if (!save) {
        return save;
    }
    meta_ = std::move(next_meta);
    return Result<void>::Ok();
}

void RaftNode::ResetPeerProgressLocked(uint64_t next_index) {
    std::unique_lock lock(peer_progress_mutex_);
    peer_progress_.clear();
    const auto now = std::chrono::steady_clock::now();
    for (const auto& peer : config_.cluster.peers) {
        if (peer.empty() || peer == self_id_ || peer == self_addr_) {
            continue;
        }

        PeerProgress progress;
        progress.peer_id = peer;
        progress.next_index = next_index;
        progress.match_index = 0;
        progress.in_flight = 0;
        progress.healthy = true;
        progress.last_ack_time = now;
        peer_progress_[peer] = progress;
    }
}

std::string RaftNode::MetaPath() const {
    return (std::filesystem::path(config_.storage.raft_log_dir) / "meta.bin").string();
}

void RaftNode::RegisterConfiguredPeers() {
    topology_->SetSelf(self_id_, self_addr_);
    topology_->RegisterNode(self_id_, self_addr_);
    for (const auto& peer : config_.cluster.peers) {
        // 当前阶段配置里 peers 仍是地址列表，这里先把它同时视为 peer 唯一键和地址。
        // 后续若配置升级为显式 node_id/addr 映射，只需要在这里替换注册逻辑。
        topology_->RegisterNode(peer, peer);
    }
}
