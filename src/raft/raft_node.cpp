#include "raft/raft_node.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <system_error>
#include <utility>

#include "common/logger.hpp"
#include "storage/snapshot_store.hpp"

namespace {

uint64_t SegmentSizeBytes(const StorageConfig& storage) {
    return static_cast<uint64_t>(storage.wal_segment_size_mb) * 1024ULL * 1024ULL;
}

std::chrono::milliseconds RandomElectionTimeout(const RaftConfig& raft) {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<uint32_t> dist(raft.election_timeout_min_ms,
                                                 raft.election_timeout_max_ms);
    return std::chrono::milliseconds(dist(rng));
}

Result<std::string> ExtractRequestId(const LogEntry& entry) {
    if (entry.type != EntryType::kNormal) {
        return Result<std::string>::Ok({});
    }

    if (entry.cmd_type == CmdType::kUpsert) {
        auto command = UpsertCmd::Deserialize(entry.payload);
        if (!command) {
            return Result<std::string>::Err(command.error);
        }
        return Result<std::string>::Ok(command->request_id);
    }

    if (entry.cmd_type == CmdType::kDelete) {
        auto command = DeleteCmd::Deserialize(entry.payload);
        if (!command) {
            return Result<std::string>::Err(command.error);
        }
        return Result<std::string>::Ok(command->request_id);
    }

    return Result<std::string>::Ok({});
}

std::string BuildSnapshotCreatedAt() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm utc_time{};
#if defined(_WIN32)
    gmtime_s(&utc_time, &now_time);
#else
    gmtime_r(&now_time, &utc_time);
#endif

    std::ostringstream stream;
    stream << std::put_time(&utc_time, "%Y-%m-%dT%H:%M:%SZ");
    return stream.str();
}

std::string DedupSnapshotPath(const StorageConfig& storage) {
    return (std::filesystem::path(storage.snapshot_dir) / "dedup.bin").string();
}

std::string TemporaryDedupSnapshotPath(const StorageConfig& storage) {
    return DedupSnapshotPath(storage) + ".tmp";
}

constexpr std::size_t kInstallSnapshotChunkBytes = 256U * 1024U;

Result<std::vector<float>> ParseFloatVectorBytes(const std::string& bytes) {
    if (bytes.empty()) {
        return Result<std::vector<float>>::Ok({});
    }
    if (bytes.size() % sizeof(float) != 0U) {
        return Result<std::vector<float>>::Err("向量字节长度不是 float32 的整数倍");
    }

    const std::size_t count = bytes.size() / sizeof(float);
    std::vector<float> values(count, 0.0F);
    std::memcpy(values.data(), bytes.data(), bytes.size());
    return Result<std::vector<float>>::Ok(std::move(values));
}

std::string NodeRoleName(NodeRole role) {
    switch (role) {
        case NodeRole::kLeader:
            return "leader";
        case NodeRole::kMentor:
            return "mentor";
        case NodeRole::kFollower:
            return "follower";
    }
    return "follower";
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
      index_maintenance_(std::make_unique<IndexMaintenance>(config_.index_maintenance)),
      vector_index_(std::move(options.vector_index)),
      dedup_table_(std::move(options.dedup_table)),
      raft_client_(std::move(options.raft_client)) {
    voted_for_ = meta_.voted_for;
    last_snapshot_index_.store(options.restored_snapshot_index, std::memory_order_release);
    last_snapshot_term_.store(options.restored_snapshot_term, std::memory_order_release);
    if (options.restored_snapshot_index > 0) {
        applied_index_.store(options.restored_snapshot_index, std::memory_order_release);
        commit_index_.store(options.restored_snapshot_index, std::memory_order_release);
    }
    const auto now = std::chrono::steady_clock::now();
    election_deadline_ = now + RandomElectionTimeout(config_.raft);
    last_leader_contact_ = now;
    last_topology_refresh_ = now;
    last_index_maintenance_check_ = now;
}

RaftNode::~RaftNode() {
    Stop();
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

    SnapshotStore snapshot_store(config.storage.snapshot_dir);
    auto init_snapshot_store = snapshot_store.Initialize();
    if (!init_snapshot_store) {
        return Result<std::shared_ptr<RaftNode>>::Err(init_snapshot_store.error);
    }

    auto topology = std::make_unique<TopologyManager>(config.cluster.node_id, options.self_addr);
    auto lease = std::make_unique<LeaseManager>();

    if (!options.vector_index) {
        auto index = VectorIndex::Create(config.vector);
        if (!index) {
            return Result<std::shared_ptr<RaftNode>>::Err(index.error);
        }
        options.vector_index = *index;
    }
    if (!options.dedup_table) {
        options.dedup_table = std::make_shared<DedupTable>(config.client.dedup_window_size);
    }
    if (!options.raft_client) {
        options.raft_client = std::make_shared<RaftClient>();
    }

    const bool auto_start = options.auto_start_background_loops;
    auto node = std::shared_ptr<RaftNode>(
        new RaftNode(config, std::move(options), std::move(*wal), std::move(*meta),
                     std::move(topology), std::move(lease)));
    node->RegisterConfiguredPeers();
    if (auto_start) {
        auto start = node->Start();
        if (!start) {
            return Result<std::shared_ptr<RaftNode>>::Err(start.error);
        }
    }
    return Result<std::shared_ptr<RaftNode>>::Ok(std::move(node));
}

Result<void> RaftNode::Start() {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (running_.exchange(true)) {
        return Result<void>::Ok();
    }

    election_thread_ = std::jthread([this](std::stop_token stop_token) { ElectionLoop(stop_token); });
    heartbeat_thread_ =
        std::jthread([this](std::stop_token stop_token) { HeartbeatLoop(stop_token); });
    replication_thread_ =
        std::jthread([this](std::stop_token stop_token) { ReplicationLoop(stop_token); });
    apply_thread_ = std::jthread([this](std::stop_token stop_token) { ApplyLoop(stop_token); });
    maintenance_thread_ =
        std::jthread([this](std::stop_token stop_token) { MaintenanceLoop(stop_token); });

    // 单节点模式下不需要等待选举超时，启动后立即成为 Leader。
    if (config_.cluster.peers.empty()) {
        auto become_leader = BecomeLeader();
        if (!become_leader) {
            running_.store(false);
            return become_leader;
        }
        lease_->Renew(std::chrono::milliseconds(config_.raft.lease_duration_ms));
        auto maybe_commit = MaybeCommit();
        if (!maybe_commit) {
            running_.store(false);
            return maybe_commit;
        }
    }

    return Result<void>::Ok();
}

void RaftNode::Stop() {
    std::jthread election_thread;
    std::jthread heartbeat_thread;
    std::jthread replication_thread;
    std::jthread apply_thread;
    std::jthread maintenance_thread;

    {
        std::lock_guard lifecycle_lock(lifecycle_mutex_);
        if (!running_.exchange(false)) {
            return;
        }

        election_thread = std::move(election_thread_);
        heartbeat_thread = std::move(heartbeat_thread_);
        replication_thread = std::move(replication_thread_);
        apply_thread = std::move(apply_thread_);
        maintenance_thread = std::move(maintenance_thread_);
    }

    election_thread.request_stop();
    heartbeat_thread.request_stop();
    replication_thread.request_stop();
    apply_thread.request_stop();
    maintenance_thread.request_stop();

    const auto current_thread_id = std::this_thread::get_id();
    if (election_thread.joinable() && election_thread.get_id() == current_thread_id) {
        election_thread.detach();
    }
    if (heartbeat_thread.joinable() && heartbeat_thread.get_id() == current_thread_id) {
        heartbeat_thread.detach();
    }
    if (replication_thread.joinable() && replication_thread.get_id() == current_thread_id) {
        replication_thread.detach();
    }
    if (apply_thread.joinable() && apply_thread.get_id() == current_thread_id) {
        apply_thread.detach();
    }
    if (maintenance_thread.joinable() && maintenance_thread.get_id() == current_thread_id) {
        maintenance_thread.detach();
    }

    replicate_cv_.notify_all();
    apply_cv_.notify_all();
    heartbeat_cv_.notify_all();
}

bool RaftNode::IsRunning() const {
    return running_.load(std::memory_order_acquire);
}

Result<uint64_t> RaftNode::Propose(const LogEntry& entry) {
    std::lock_guard state_lock(state_mutex_);
    if (state_.load(std::memory_order_acquire) != RaftState::kLeader) {
        return Result<uint64_t>::Err("当前节点不是 Leader，无法执行 Propose");
    }

    auto appended = AppendEntryLocked(entry);
    if (!appended) {
        return appended;
    }

    auto request_id = ExtractRequestId(entry);
    if (request_id && !request_id->empty()) {
        dedup_table_->TrackPending(*request_id, *appended);
    }

    if (QuorumSize() == 1U) {
        commit_index_.store(*appended, std::memory_order_release);
        apply_cv_.notify_all();
    } else {
        RequestReplication();
        RequestImmediateHeartbeat();
    }

    return appended;
}

Result<uint64_t> RaftNode::LeaseRead() {
    auto readable = EnsureLeaseReadable();
    if (!readable) {
        return Result<uint64_t>::Err(readable.error);
    }
    return Result<uint64_t>::Ok(commit_index_.load(std::memory_order_acquire));
}

Result<std::vector<VectorIndex::SearchResult>> RaftNode::LeaseRead(const float* vec,
                                                                   size_t dim,
                                                                   size_t top_k) {
    auto readable = EnsureLeaseReadable();
    if (!readable) {
        return Result<std::vector<VectorIndex::SearchResult>>::Err(readable.error);
    }

    std::shared_ptr<VectorIndex> vector_index;
    {
        std::shared_lock index_lock(vector_index_mutex_);
        vector_index = vector_index_;
    }
    if (!vector_index) {
        return Result<std::vector<VectorIndex::SearchResult>>::Err("向量索引尚未初始化");
    }
    return vector_index->Search(vec, dim, top_k);
}

Result<raftvdb::proto::AppendEntriesResponse> RaftNode::HandleAppendEntries(
    const raftvdb::proto::AppendEntriesRequest& request) {
    raftvdb::proto::AppendEntriesResponse response;
    response.set_node_id(self_addr_.empty() ? self_id_ : self_addr_);

    uint64_t current_term = CurrentTerm();
    if (request.term() < current_term) {
        response.set_term(current_term);
        response.set_success(false);
        response.set_conflict_index(LogicalLastIndex() + 1U);
        return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
    }

    {
        std::lock_guard state_lock(state_mutex_);
        current_term = current_term_.load(std::memory_order_acquire);
        if (request.term() > current_term ||
            state_.load(std::memory_order_acquire) != RaftState::kFollower) {
            // 真实的降级路径仍复用 BecomeFollower，确保 term / voted_for / lease 处理一致。
        } else {
            leader_id_ = request.leader_id();
            topology_->SetLeader(request.leader_id());
            NoteLeaderContactLocked();
        }
    }
    if (request.term() > current_term ||
        state_.load(std::memory_order_acquire) != RaftState::kFollower) {
        auto become_follower = BecomeFollower(request.term(), request.leader_id());
        if (!become_follower) {
            return Result<raftvdb::proto::AppendEntriesResponse>::Err(become_follower.error);
        }
    }

    if (!request.topology().role().empty()) {
        auto apply_topology = topology_->FromProto(self_id_, request.topology(), self_addr_);
        if (!apply_topology) {
            return Result<raftvdb::proto::AppendEntriesResponse>::Err(apply_topology.error);
        }
    }

    const uint64_t prev_log_index = request.prev_log_index();
    const uint64_t snapshot_index = last_snapshot_index_.load(std::memory_order_acquire);
    const uint64_t snapshot_term = last_snapshot_term_.load(std::memory_order_acquire);
    const uint64_t logical_last_index = LogicalLastIndex();
    if (prev_log_index > logical_last_index) {
        response.set_term(CurrentTerm());
        response.set_success(false);
        response.set_conflict_index(logical_last_index + 1U);
        return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
    }

    if (snapshot_index > 0 && prev_log_index < snapshot_index) {
        response.set_term(CurrentTerm());
        response.set_success(false);
        response.set_conflict_index(snapshot_index + 1U);
        response.set_conflict_term(snapshot_term);
        return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
    }

    if (prev_log_index > 0) {
        auto prev_term = TermAtLogicalIndex(prev_log_index);
        if (!prev_term) {
            response.set_term(CurrentTerm());
            response.set_success(false);
            response.set_conflict_index(std::max<uint64_t>(snapshot_index + 1U, 1U));
            return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
        }

        if (*prev_term != request.prev_log_term()) {
            response.set_term(CurrentTerm());
            response.set_success(false);
            response.set_conflict_term(*prev_term);

            if (snapshot_index > 0 && prev_log_index == snapshot_index) {
                response.set_conflict_index(snapshot_index);
                return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
            }

            uint64_t conflict_index = prev_log_index;
            while (conflict_index > snapshot_index + 1U) {
                auto previous = wal_->Read(conflict_index - 1U);
                if (!previous || previous->term != *prev_term) {
                    break;
                }
                --conflict_index;
            }
            response.set_conflict_index(conflict_index);
            return Result<raftvdb::proto::AppendEntriesResponse>::Ok(std::move(response));
        }
    }

    bool appended_any = false;
    for (int index = 0; index < request.entries_size(); ++index) {
        LogEntry incoming;
        incoming.index = request.entries(index).index();
        incoming.term = request.entries(index).term();
        incoming.type = static_cast<EntryType>(request.entries(index).type());
        incoming.cmd_type = static_cast<CmdType>(request.entries(index).cmd_type());
        incoming.payload = request.entries(index).payload();

        if (snapshot_index > 0 && incoming.index <= snapshot_index) {
            continue;
        }

        auto existing = wal_->Read(incoming.index);
        if (existing) {
            if (existing->term != incoming.term) {
                auto truncate = wal_->TruncateSuffix(incoming.index);
                if (!truncate) {
                    return Result<raftvdb::proto::AppendEntriesResponse>::Err(truncate.error);
                }
            } else {
                continue;
            }
        }

        auto append_result = wal_->Append(incoming);
        if (!append_result) {
            return Result<raftvdb::proto::AppendEntriesResponse>::Err(append_result.error);
        }
        appended_any = true;
    }

    if (appended_any) {
        auto flush_result = wal_->Flush();
        if (!flush_result) {
            return Result<raftvdb::proto::AppendEntriesResponse>::Err(flush_result.error);
        }
    }

    const uint64_t bounded_commit =
        std::min<uint64_t>(request.leader_commit(), LogicalLastIndex());
    const uint64_t previous_commit = commit_index_.load(std::memory_order_acquire);
    if (bounded_commit > previous_commit) {
        commit_index_.store(bounded_commit, std::memory_order_release);
        apply_cv_.notify_all();
    }

    // Mentor 本地收到日志后，会再把相同日志转发给自己的下游 Follower。
    // 这里选择异步转发，避免把 Leader -> Mentor 的 RPC 时延和下游网络耦合在一起。
    auto self_role = topology_->GetRole(self_id_);
    if (self_role == NodeRole::kMentor && request.entries_size() > 0) {
        auto weak_self = weak_from_this();
        const auto request_copy = request;
        std::thread([weak_self, request_copy]() {
            if (auto self = weak_self.lock()) {
                auto ignored = self->ForwardToFollower(request_copy);
                (void)ignored;
            }
        }).detach();
    }

    response.set_term(CurrentTerm());
    response.set_success(true);
    response.set_match_index(LogicalLastIndex());
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

    {
        std::lock_guard state_lock(state_mutex_);
        const bool can_vote = voted_for_.empty() || voted_for_ == request.candidate_id();
        const bool log_is_up_to_date =
            IsCandidateLogUpToDateLocked(request.last_log_index(), request.last_log_term());

        response.set_term(current_term_.load(std::memory_order_acquire));
        if (can_vote && log_is_up_to_date) {
            auto persist = PersistMetaLocked(request.term(), request.candidate_id());
            if (!persist) {
                return Result<raftvdb::proto::RequestVoteResponse>::Err(persist.error);
            }
            voted_for_ = request.candidate_id();
            state_.store(RaftState::kFollower, std::memory_order_release);
            NoteLeaderContactLocked();
            response.set_vote_granted(true);
            return Result<raftvdb::proto::RequestVoteResponse>::Ok(std::move(response));
        }
    }

    response.set_term(CurrentTerm());
    response.set_vote_granted(false);
    return Result<raftvdb::proto::RequestVoteResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::HeartbeatResponse> RaftNode::HandleHeartbeat(
    const raftvdb::proto::HeartbeatRequest& request) {
    raftvdb::proto::HeartbeatResponse response;
    response.set_node_id(self_addr_.empty() ? self_id_ : self_addr_);

    uint64_t current_term = CurrentTerm();
    if (request.term() < current_term) {
        response.set_term(current_term);
        response.set_success(false);
        return Result<raftvdb::proto::HeartbeatResponse>::Ok(std::move(response));
    }

    {
        std::lock_guard state_lock(state_mutex_);
        current_term = current_term_.load(std::memory_order_acquire);
        if (request.term() > current_term ||
            state_.load(std::memory_order_acquire) != RaftState::kFollower) {
            // 需要在锁外复用 BecomeFollower，以统一处理持久化和租约失效。
        } else {
            leader_id_ = request.leader_id();
            topology_->SetLeader(request.leader_id());
            NoteLeaderContactLocked();
        }
    }
    if (request.term() > current_term ||
        state_.load(std::memory_order_acquire) != RaftState::kFollower) {
        auto become_follower = BecomeFollower(request.term(), request.leader_id());
        if (!become_follower) {
            return Result<raftvdb::proto::HeartbeatResponse>::Err(become_follower.error);
        }
    }

    if (!request.topology().role().empty()) {
        auto apply_topology = topology_->FromProto(self_id_, request.topology(), self_addr_);
        if (!apply_topology) {
            return Result<raftvdb::proto::HeartbeatResponse>::Err(apply_topology.error);
        }
    }

    const uint64_t bounded_commit =
        std::min<uint64_t>(request.commit_index(), LogicalLastIndex());
    const uint64_t previous_commit = commit_index_.load(std::memory_order_acquire);
    if (bounded_commit > previous_commit) {
        commit_index_.store(bounded_commit, std::memory_order_release);
        apply_cv_.notify_all();
    }

    response.set_term(CurrentTerm());
    response.set_success(true);
    return Result<raftvdb::proto::HeartbeatResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::InstallSnapshotResponse> RaftNode::HandleInstallSnapshot(
    grpc::ServerReader<raftvdb::proto::SnapshotChunk>* reader) {
    raftvdb::proto::InstallSnapshotResponse response;
    response.set_term(CurrentTerm());

    if (reader == nullptr) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "InstallSnapshot 失败: reader 不能为空");
    }

    SnapshotStore store(config_.storage.snapshot_dir);
    auto init_store = store.Initialize();
    if (!init_store) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(init_store.error);
    }

    const std::string temp_dedup_path = TemporaryDedupSnapshotPath(config_.storage);
    const auto cleanup_temporary_files = [&]() {
        std::error_code snapshot_ec;
        std::error_code dedup_ec;
        std::filesystem::remove(store.TemporarySnapshotPath(), snapshot_ec);
        std::filesystem::remove(temp_dedup_path, dedup_ec);
    };

    raftvdb::proto::SnapshotChunk chunk;
    SnapshotMeta meta;
    bool has_chunk = false;
    bool saw_snapshot_file = false;
    bool saw_dedup_file = false;
    bool stream_finished = false;
    std::string current_file_name;
    std::ofstream current_output;
    uint64_t expected_offset = 0;

    while (reader->Read(&chunk)) {
        if (!has_chunk) {
            has_chunk = true;
            const uint64_t incoming_term = chunk.raft_term();
            if (incoming_term < CurrentTerm()) {
                response.set_term(CurrentTerm());
                response.set_success(false);
                cleanup_temporary_files();
                return Result<raftvdb::proto::InstallSnapshotResponse>::Ok(std::move(response));
            }

            if (incoming_term > CurrentTerm() ||
                state_.load(std::memory_order_acquire) != RaftState::kFollower) {
                auto become_follower = BecomeFollower(incoming_term);
                if (!become_follower) {
                    cleanup_temporary_files();
                    return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                        become_follower.error);
                }
            }

            meta.raft_term = chunk.raft_term();
            meta.raft_index = chunk.raft_index();
            meta.dim = chunk.dim();
            meta.metric = chunk.metric();
            meta.data_type = chunk.data_type();
            meta.created_at = BuildSnapshotCreatedAt();
            LOG_INFO("INSTALL_SNAPSHOT_BEGIN", "node_id={}, index={}, term={}", self_id_,
                     meta.raft_index, meta.raft_term);
            response.set_term(CurrentTerm());
        } else if (chunk.raft_term() != meta.raft_term || chunk.raft_index() != meta.raft_index ||
                   chunk.dim() != meta.dim || chunk.metric() != meta.metric ||
                   chunk.data_type() != meta.data_type) {
            cleanup_temporary_files();
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                "InstallSnapshot 失败: 快照 chunk 元数据不一致");
        }

        std::string file_name = chunk.file_name();
        if (file_name.empty()) {
            file_name = "snapshot.usearch";
        }

        std::string target_path;
        if (file_name == "snapshot.usearch") {
            target_path = store.TemporarySnapshotPath();
            saw_snapshot_file = true;
        } else if (file_name == "dedup.bin") {
            target_path = temp_dedup_path;
            saw_dedup_file = true;
        } else {
            cleanup_temporary_files();
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                "InstallSnapshot 失败: 不支持的快照文件 " + file_name);
        }

        if (current_file_name != file_name) {
            if (current_output.is_open()) {
                current_output.close();
            }
            if (chunk.offset() != 0) {
                cleanup_temporary_files();
                return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                    "InstallSnapshot 失败: 新文件的首个 chunk offset 必须为 0");
            }

            current_output.open(target_path, std::ios::binary | std::ios::trunc);
            if (!current_output.is_open()) {
                cleanup_temporary_files();
                return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                    "InstallSnapshot 失败: 打开临时文件失败 " + target_path);
            }
            current_file_name = file_name;
            expected_offset = 0;
        }

        if (chunk.offset() != expected_offset) {
            cleanup_temporary_files();
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                "InstallSnapshot 失败: chunk offset 不连续");
        }

        current_output.write(chunk.data().data(), static_cast<std::streamsize>(chunk.data().size()));
        if (!current_output.good()) {
            cleanup_temporary_files();
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                "InstallSnapshot 失败: 写入临时文件失败");
        }

        expected_offset += chunk.data().size();
        stream_finished = chunk.is_last();
    }

    if (current_output.is_open()) {
        current_output.close();
    }

    if (!has_chunk) {
        response.set_success(false);
        cleanup_temporary_files();
        return Result<raftvdb::proto::InstallSnapshotResponse>::Ok(std::move(response));
    }
    if (!stream_finished) {
        cleanup_temporary_files();
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "InstallSnapshot 失败: 快照流在 is_last 前中断");
    }
    if (!saw_snapshot_file) {
        cleanup_temporary_files();
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "InstallSnapshot 失败: 缺少 snapshot.usearch");
    }
    if (!saw_dedup_file) {
        cleanup_temporary_files();
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "InstallSnapshot 失败: 缺少 dedup.bin");
    }

    auto finalize_snapshot = store.FinalizeSnapshot(meta);
    if (!finalize_snapshot) {
        cleanup_temporary_files();
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(finalize_snapshot.error);
    }

    std::error_code dedup_rename_ec;
    std::filesystem::rename(temp_dedup_path, DedupSnapshotPath(config_.storage), dedup_rename_ec);
    if (dedup_rename_ec) {
        std::error_code cleanup_ec;
        std::filesystem::remove(temp_dedup_path, cleanup_ec);
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "InstallSnapshot 失败: 提交 dedup.bin 失败: " + dedup_rename_ec.message());
    }

    auto apply_snapshot = ApplyInstalledSnapshot(meta);
    if (!apply_snapshot) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(apply_snapshot.error);
    }

    response.set_term(CurrentTerm());
    response.set_success(true);
    LOG_INFO("INSTALL_SNAPSHOT_COMPLETED", "node_id={}, index={}, term={}", self_id_,
             meta.raft_index, meta.raft_term);
    return Result<raftvdb::proto::InstallSnapshotResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::ClientWriteResponse> RaftNode::HandleClientWrite(
    const raftvdb::proto::ClientWriteRequest& request) {
    raftvdb::proto::ClientWriteResponse response;

    // 客户端写入口只接受 Leader。
    // Follower 不直接报 transport error，而是显式返回 redirect_to，
    // 让 DBClient 能在同一次逻辑调用里做单跳切换。
    const auto leader_hint = LeaderAddr();
    if (!IsLeader()) {
        response.set_success(false);
        response.set_redirect_to(leader_hint);
        return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
    }

    if (request.request_id().empty()) {
        response.set_success(false);
        response.set_error("request_id 不能为空");
        return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
    }

    DedupEntry dedup_entry;
    auto dedup_state = dedup_table_->Check(request.request_id(), &dedup_entry);
    if (dedup_state == DedupTable::CheckResult::kAlreadyCommitted) {
        response.set_success(dedup_entry.success);
        response.set_error(dedup_entry.error);
        return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
    }

    if (dedup_state == DedupTable::CheckResult::kNotFound) {
        LogEntry entry;
        entry.type = EntryType::kNormal;

        if (request.cmd_type() == 1U) {
            auto vector = ParseFloatVectorBytes(request.vector());
            if (!vector) {
                response.set_success(false);
                response.set_error(vector.error);
                return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
            }
            if (vector->size() != config_.vector.dim) {
                response.set_success(false);
                response.set_error("写入向量维度与当前配置不一致");
                return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
            }

            UpsertCmd command;
            command.id = request.id();
            command.request_id = request.request_id();
            command.vector = *vector;
            entry.cmd_type = CmdType::kUpsert;
            entry.payload = command.Serialize();
        } else if (request.cmd_type() == 2U) {
            DeleteCmd command;
            command.id = request.id();
            command.request_id = request.request_id();
            entry.cmd_type = CmdType::kDelete;
            entry.payload = command.Serialize();
        } else {
            response.set_success(false);
            response.set_error("不支持的 cmd_type");
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
        }

        auto proposed = Propose(entry);
        if (!proposed) {
            response.set_success(false);
            if (!IsLeader()) {
                response.set_redirect_to(LeaderAddr());
            } else {
                response.set_error(proposed.error);
            }
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
        }
    }

    // 已经写入 WAL 后，不立即把“成功”返回给客户端，而是等待这条请求真正进入
    // DedupTable 的 committed 状态。这样客户端只会在 apply 完成后看到 success=true，
    // 后续重复请求也能直接走去重表返回。
    const auto wait_timeout = std::chrono::milliseconds(
        std::max<uint32_t>(config_.client.retry_max_ms * 2U, config_.raft.election_timeout_max_ms * 2U));
    const auto deadline = std::chrono::steady_clock::now() + wait_timeout;

    while (std::chrono::steady_clock::now() < deadline) {
        auto current_state = dedup_table_->Check(request.request_id(), &dedup_entry);
        if (current_state == DedupTable::CheckResult::kAlreadyCommitted) {
            response.set_success(dedup_entry.success);
            response.set_error(dedup_entry.error);
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
        }

        if (!IsLeader()) {
            response.set_success(false);
            response.set_redirect_to(LeaderAddr());
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
        }

        std::unique_lock apply_lock(apply_mutex_);
        apply_cv_.wait_for(apply_lock, std::chrono::milliseconds(20));
    }

    response.set_success(false);
    if (!IsLeader()) {
        response.set_redirect_to(LeaderAddr());
    }
    return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(response));
}

Result<raftvdb::proto::ClientSearchResponse> RaftNode::HandleClientSearch(
    const raftvdb::proto::ClientSearchRequest& request) {
    raftvdb::proto::ClientSearchResponse response;

    // 搜索与写入一样，只接受 Leader 入口。
    // 这样 DBClient 不需要区分读写的寻主逻辑，统一跟随 redirect_to 即可。
    const auto leader_hint = LeaderAddr();
    if (!IsLeader()) {
        response.set_success(false);
        response.set_redirect_to(leader_hint);
        return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
    }

    if (request.top_k() == 0U) {
        response.set_success(false);
        response.set_error("top_k 必须大于 0");
        return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
    }

    auto vector = ParseFloatVectorBytes(request.vector());
    if (!vector) {
        response.set_success(false);
        response.set_error(vector.error);
        return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
    }
    if (vector->size() != config_.vector.dim) {
        response.set_success(false);
        response.set_error("搜索向量维度与当前配置不一致");
        return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
    }

    auto searched = LeaseRead(vector->data(), vector->size(), request.top_k());
    if (!searched) {
        response.set_success(false);
        if (!IsLeader()) {
            response.set_redirect_to(LeaderAddr());
        } else {
            response.set_error(searched.error);
        }
        return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
    }

    response.set_success(true);
    for (const auto& hit : *searched) {
        auto* proto_hit = response.add_hits();
        proto_hit->set_id(hit.id);
        proto_hit->set_distance(hit.distance);
    }
    return Result<raftvdb::proto::ClientSearchResponse>::Ok(std::move(response));
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
    std::lock_guard state_lock(state_mutex_);
    return leader_id_;
}

std::string RaftNode::LeaderAddr() const {
    std::string leader_id;
    {
        std::lock_guard state_lock(state_mutex_);
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
    std::lock_guard state_lock(state_mutex_);
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
    topology_->SetLeader(leader_id);
    NoteLeaderContactLocked();

    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        peer_progress_.clear();
    }

    LOG_INFO("RAFT_BECOME_FOLLOWER", "node_id={}, term={}, leader_id={}", self_id_, term, leader_id);
    return Result<void>::Ok();
}

Result<void> RaftNode::BecomeCandidate() {
    std::lock_guard state_lock(state_mutex_);
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
    ResetElectionDeadlineLocked();

    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        peer_progress_.clear();
    }

    LOG_INFO("RAFT_BECOME_CANDIDATE", "node_id={}, term={}", self_id_, next_term);
    return Result<void>::Ok();
}

Result<void> RaftNode::BecomeLeader() {
    std::lock_guard state_lock(state_mutex_);
    uint64_t term = current_term_.load(std::memory_order_acquire);
    if (term == 0) {
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
    auto rebalance = topology_->Rebalance(config_.cluster.peers);
    if (!rebalance) {
        return rebalance;
    }

    auto noop_index = AppendNoopEntryLocked();
    if (!noop_index) {
        return Result<void>::Err(noop_index.error);
    }

    ResetPeerProgressLocked(*noop_index + 1);
    state_.store(RaftState::kLeader, std::memory_order_release);
    lease_->Invalidate();
    last_topology_refresh_ = std::chrono::steady_clock::now();

    LOG_INFO("RAFT_BECOME_LEADER", "node_id={}, term={}, noop_index={}", self_id_, term,
             *noop_index);
    RequestReplication();
    RequestImmediateHeartbeat();
    return Result<void>::Ok();
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

Result<void> RaftNode::StartElection() {
    auto become_candidate = BecomeCandidate();
    if (!become_candidate) {
        return become_candidate;
    }

    raftvdb::proto::RequestVoteRequest request;
    request.set_term(CurrentTerm());
    request.set_candidate_id(self_id_);
    request.set_last_log_index(LogicalLastIndex());
    {
        std::lock_guard state_lock(state_mutex_);
        request.set_last_log_term(LastLogTermLocked());
    }

    std::atomic<size_t> granted_votes{1};
    std::mutex response_mutex;
    uint64_t highest_term = request.term();

    std::vector<std::thread> threads;
    threads.reserve(config_.cluster.peers.size());
    for (const auto& peer : config_.cluster.peers) {
        threads.emplace_back([&, peer]() {
            auto response = raft_client_->RequestVote(ResolvePeerAddress(peer), request);
            if (!response) {
                return;
            }

            std::lock_guard lock(response_mutex);
            if (response->term() > highest_term) {
                highest_term = response->term();
            }
            if (response->vote_granted()) {
                ++granted_votes;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    if (highest_term > request.term()) {
        return BecomeFollower(highest_term);
    }

    {
        std::lock_guard state_lock(state_mutex_);
        if (state_.load(std::memory_order_acquire) != RaftState::kCandidate ||
            current_term_.load(std::memory_order_acquire) != request.term()) {
            return Result<void>::Ok();
        }
    }

    if (granted_votes.load() >= QuorumSize()) {
        auto become_leader = BecomeLeader();
        if (!become_leader) {
            return become_leader;
        }
        auto maybe_commit = MaybeCommit();
        if (!maybe_commit) {
            return maybe_commit;
        }
    }

    return Result<void>::Ok();
}

Result<void> RaftNode::BroadcastHeartbeat(bool /*allow_async_callbacks*/) {
    if (!IsLeader()) {
        return Result<void>::Ok();
    }

    const uint64_t term = CurrentTerm();
    const uint64_t commit_index = CommitIndex();
    size_t success_count = 1;
    uint64_t highest_term = term;

    for (const auto& peer : config_.cluster.peers) {
        raftvdb::proto::HeartbeatRequest request;
        request.set_term(term);
        request.set_leader_id(self_id_);
        request.set_commit_index(commit_index);

        auto topology = BuildTopologyForPeer(peer);
        if (topology) {
            *request.mutable_topology() = *topology;
        }

        auto sent_at = std::chrono::steady_clock::now();
        auto response = raft_client_->Heartbeat(ResolvePeerAddress(peer), request);
        if (!response) {
            UpdatePeerFailure(peer);
            continue;
        }
        if (response->term() > highest_term) {
            highest_term = response->term();
            continue;
        }
        if (response->success()) {
            ++success_count;
            UpdatePeerHeartbeatAck(peer, sent_at);
        } else {
            UpdatePeerFailure(peer);
        }
    }

    if (highest_term > term) {
        return BecomeFollower(highest_term);
    }

    if (success_count >= QuorumSize()) {
        lease_->Renew(std::chrono::milliseconds(config_.raft.lease_duration_ms));
    }
    return Result<void>::Ok();
}

void RaftNode::ResetElectionDeadlineLocked() {
    const auto now = std::chrono::steady_clock::now();
    last_leader_contact_ = now;
    election_deadline_ = now + RandomElectionTimeout(config_.raft);
}

void RaftNode::NoteLeaderContactLocked() {
    ResetElectionDeadlineLocked();
}

uint64_t RaftNode::LogicalLastIndex() const {
    return std::max(last_snapshot_index_.load(std::memory_order_acquire), wal_->LastIndex());
}

Result<uint64_t> RaftNode::TermAtLogicalIndex(uint64_t index) const {
    if (index == 0) {
        return Result<uint64_t>::Ok(0);
    }

    const uint64_t snapshot_index = last_snapshot_index_.load(std::memory_order_acquire);
    const uint64_t snapshot_term = last_snapshot_term_.load(std::memory_order_acquire);
    if (snapshot_index > 0 && index < snapshot_index) {
        return Result<uint64_t>::Err("日志索引已被快照截断: " + std::to_string(index));
    }
    if (snapshot_index > 0 && index == snapshot_index && snapshot_term != 0) {
        return Result<uint64_t>::Ok(snapshot_term);
    }

    auto term = wal_->TermAt(index);
    if (!term) {
        return Result<uint64_t>::Err(term.error);
    }
    return term;
}

uint64_t RaftNode::LastLogTermLocked() {
    const uint64_t last_index = LogicalLastIndex();
    if (last_index == 0) {
        return 0;
    }
    auto term = TermAtLogicalIndex(last_index);
    if (!term) {
        return 0;
    }
    return *term;
}

bool RaftNode::IsCandidateLogUpToDateLocked(uint64_t candidate_last_log_index,
                                            uint64_t candidate_last_log_term) {
    const uint64_t local_last_log_term = LastLogTermLocked();
    const uint64_t local_last_log_index = LogicalLastIndex();
    if (candidate_last_log_term != local_last_log_term) {
        return candidate_last_log_term > local_last_log_term;
    }
    return candidate_last_log_index >= local_last_log_index;
}

Result<uint64_t> RaftNode::AppendEntryLocked(const LogEntry& entry) {
    LogEntry next = entry;
    next.index = LogicalLastIndex() + 1U;
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

Result<void> RaftNode::MaybeCommit() {
    if (!IsLeader()) {
        return Result<void>::Ok();
    }

    const uint64_t last_index = LogicalLastIndex();
    uint64_t new_commit = commit_index_.load(std::memory_order_acquire);
    for (uint64_t candidate = last_index; candidate > new_commit; --candidate) {
        auto term = TermAtLogicalIndex(candidate);
        if (!term) {
            return Result<void>::Err(term.error);
        }
        if (*term != CurrentTerm()) {
            continue;
        }

        size_t matched = 1; // Leader 自身天然匹配。
        {
            std::shared_lock progress_lock(peer_progress_mutex_);
            for (const auto& [_, progress] : peer_progress_) {
                if (progress.match_index >= candidate) {
                    ++matched;
                }
            }
        }

        if (matched >= QuorumSize()) {
            new_commit = candidate;
            break;
        }
    }

    const uint64_t previous_commit = commit_index_.load(std::memory_order_acquire);
    if (new_commit > previous_commit) {
        commit_index_.store(new_commit, std::memory_order_release);
        apply_cv_.notify_all();
        RequestImmediateHeartbeat();
    }
    return Result<void>::Ok();
}

Result<void> RaftNode::ReplicatePeerOnce(const std::string& peer_id) {
    if (!IsLeader()) {
        return Result<void>::Ok();
    }

    PeerProgress snapshot;
    {
        std::shared_lock progress_lock(peer_progress_mutex_);
        auto found = peer_progress_.find(peer_id);
        if (found == peer_progress_.end()) {
            return Result<void>::Ok();
        }
        snapshot = found->second;
    }

    const uint32_t window_size =
        snapshot.effective_window_size == 0 ? config_.raft.pipeline_window_size
                                            : snapshot.effective_window_size;
    if (snapshot.in_flight >= window_size) {
        return Result<void>::Ok();
    }
    if (last_snapshot_index_.load(std::memory_order_acquire) > 0 &&
        snapshot.next_index <= last_snapshot_index_.load(std::memory_order_acquire)) {
        return SendSnapshotToPeer(peer_id);
    }

    const uint64_t logical_last_index = LogicalLastIndex();
    auto entries = wal_->ReadFrom(snapshot.next_index, config_.raft.batch_max_entries);
    if (!entries) {
        return Result<void>::Err(entries.error);
    }
    // 新 Leader 上任后，所有 peer 的 next_index 都会先被保守初始化到“本地尾部 + 1”。
    // 若此时某个节点其实是落后的，必须发送一条空 AppendEntries 作为探测包，
    // 让对端返回 conflict_index，才能继续回退到日志或快照路径。
    if (entries->empty() && snapshot.match_index >= logical_last_index) {
        return Result<void>::Ok();
    }

    raftvdb::proto::AppendEntriesRequest request;
    request.set_term(CurrentTerm());
    request.set_leader_id(self_id_);
    request.set_prev_log_index(snapshot.next_index - 1);
    auto prev_term = TermAtLogicalIndex(snapshot.next_index - 1U);
    if (!prev_term) {
        return Result<void>::Err(prev_term.error);
    }
    request.set_prev_log_term(*prev_term);
    request.set_leader_commit(CommitIndex());

    auto topology = BuildTopologyForPeer(peer_id);
    if (topology) {
        *request.mutable_topology() = *topology;
    }

    for (const auto& entry : *entries) {
        auto* proto_entry = request.add_entries();
        proto_entry->set_index(entry.index);
        proto_entry->set_term(entry.term);
        proto_entry->set_type(static_cast<uint32_t>(entry.type));
        proto_entry->set_cmd_type(static_cast<uint32_t>(entry.cmd_type));
        proto_entry->set_payload(entry.payload);
    }

    const auto sent_at = std::chrono::steady_clock::now();
    uint32_t in_flight_after_dispatch = 0;
    uint64_t optimistic_next_index = snapshot.next_index;
    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        auto& progress = peer_progress_[peer_id];
        progress.in_flight += 1;
        progress.last_send_time = sent_at;
        progress.next_index += static_cast<uint64_t>(entries->size());
        in_flight_after_dispatch = progress.in_flight;
        optimistic_next_index = progress.next_index;
    }

    LOG_DEBUG("APPEND_ENTRIES_DISPATCH",
              "leader_id={}, peer_id={}, prev_log_index={}, entries_count={}, in_flight={}, "
              "window_size={}, direct_mode={}, next_index={}",
              self_id_,
              peer_id,
              request.prev_log_index(),
              request.entries_size(),
              in_flight_after_dispatch,
              window_size,
              snapshot.direct_mode,
              optimistic_next_index);

    auto weak_self = weak_from_this();
    const auto request_copy = request;
    auto async_result = raft_client_->AppendEntriesAsync(
        ResolvePeerAddress(peer_id), request,
        [weak_self, peer_id, sent_at, request_copy](Result<raftvdb::proto::AppendEntriesResponse> result) {
            auto self = weak_self.lock();
            if (!self) {
                return;
            }
            if (!result) {
                self->UpdatePeerFailure(peer_id, request_copy.prev_log_index() + 1U);
                self->RequestReplication();
                return;
            }

            if (result->term() > self->CurrentTerm()) {
                auto ignored = self->BecomeFollower(result->term());
                (void)ignored;
                return;
            }
            if (!self->IsLeader()) {
                return;
            }

            if (result->success()) {
                self->UpdatePeerAck(peer_id, result->match_index(), sent_at);
                auto maybe_commit = self->MaybeCommit();
                (void)maybe_commit;
            } else {
                std::unique_lock progress_lock(self->peer_progress_mutex_);
                auto found = self->peer_progress_.find(peer_id);
                if (found != self->peer_progress_.end()) {
                    found->second.in_flight = 0;
                    found->second.healthy = true;
                    self->ApplyPeerRollbackStateLocked(found->second,
                                                       request_copy.prev_log_index() + 1U,
                                                       result->conflict_index());
                }
            }
            self->RequestReplication();
        });

    if (!async_result) {
        UpdatePeerFailure(peer_id, request.prev_log_index() + 1U);
        return Result<void>::Err(async_result.error);
    }

    return Result<void>::Ok();
}

Result<void> RaftNode::ForwardToFollower(const raftvdb::proto::AppendEntriesRequest& request) {
    auto follower = topology_->GetFollowerOf(self_id_);
    if (!follower || !follower->healthy) {
        return Result<void>::Ok();
    }

    {
        std::shared_lock progress_lock(peer_progress_mutex_);
        auto found = peer_progress_.find(follower->node_id);
        if (found != peer_progress_.end() &&
            last_snapshot_index_.load(std::memory_order_acquire) > 0 &&
            found->second.next_index <= last_snapshot_index_.load(std::memory_order_acquire)) {
            progress_lock.unlock();
            return SendSnapshotToPeer(follower->node_id);
        }
    }

    raftvdb::proto::AppendEntriesRequest forwarded = request;
    auto topology = BuildTopologyForPeer(follower->node_id);
    if (topology) {
        *forwarded.mutable_topology() = *topology;
    } else {
        forwarded.clear_topology();
    }

    const auto sent_at = std::chrono::steady_clock::now();
    auto response = raft_client_->AppendEntries(ResolvePeerAddress(follower->node_id), forwarded);
    if (!response) {
        UpdatePeerFailure(follower->node_id);
        return Result<void>::Err(response.error);
    }

    if (response->term() > CurrentTerm()) {
        auto become_follower = BecomeFollower(response->term());
        if (!become_follower) {
            return become_follower;
        }
        return Result<void>::Ok();
    }

    if (response->success()) {
        UpdatePeerAck(follower->node_id, response->match_index(), sent_at);
        return Result<void>::Ok();
    }

    std::unique_lock progress_lock(peer_progress_mutex_);
    auto& progress = peer_progress_[follower->node_id];
    progress.peer_id = follower->node_id;
    progress.in_flight = 0;
    progress.healthy = true;
    ApplyPeerRollbackStateLocked(progress, forwarded.prev_log_index() + 1U,
                                 response->conflict_index());
    return Result<void>::Ok();
}

Result<void> RaftNode::SendSnapshotToPeer(const std::string& peer_id) {
    SnapshotStore store(config_.storage.snapshot_dir);
    auto meta = store.LoadLatest(config_.vector);
    if (!meta) {
        return Result<void>::Err("发送快照失败: " + meta.error);
    }

    std::ifstream snapshot_input(store.SnapshotPath(), std::ios::binary);
    if (!snapshot_input.is_open()) {
        return Result<void>::Err("发送快照失败: 打开 snapshot.usearch 失败");
    }

    std::ifstream dedup_input(DedupSnapshotPath(config_.storage), std::ios::binary);
    if (!dedup_input.is_open()) {
        return Result<void>::Err("发送快照失败: 打开 dedup.bin 失败");
    }

    struct StreamFile {
        std::string name;
        std::ifstream* stream = nullptr;
    };

    std::vector<StreamFile> files{
        StreamFile{"snapshot.usearch", &snapshot_input},
        StreamFile{"dedup.bin", &dedup_input},
    };

    std::size_t file_index = 0;
    bool emitted_terminal_chunk = false;
    auto response = raft_client_->InstallSnapshot(
        ResolvePeerAddress(peer_id),
        [&, meta_copy = *meta](raftvdb::proto::SnapshotChunk& chunk) mutable -> Result<bool> {
            if (emitted_terminal_chunk) {
                return Result<bool>::Ok(false);
            }

            while (file_index < files.size()) {
                auto& file = files[file_index];
                file.stream->clear();
                const auto offset = static_cast<uint64_t>(file.stream->tellg() >= 0
                                                              ? file.stream->tellg()
                                                              : std::streampos(0));

                std::string buffer(kInstallSnapshotChunkBytes, '\0');
                file.stream->read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                const auto bytes_read = file.stream->gcount();

                if (bytes_read == 0) {
                    if (file.stream->eof()) {
                        if (offset == 0) {
                            chunk.Clear();
                            chunk.set_file_name(file.name);
                            chunk.set_offset(0);
                            chunk.set_is_last(file_index + 1 == files.size());
                            chunk.set_raft_term(meta_copy.raft_term);
                            chunk.set_raft_index(meta_copy.raft_index);
                            chunk.set_dim(meta_copy.dim);
                            chunk.set_metric(meta_copy.metric);
                            chunk.set_data_type(meta_copy.data_type);
                            if (chunk.is_last()) {
                                emitted_terminal_chunk = true;
                            }
                            ++file_index;
                            return Result<bool>::Ok(true);
                        }
                        ++file_index;
                        continue;
                    }
                    return Result<bool>::Err("发送快照失败: 读取快照文件失败");
                }

                buffer.resize(static_cast<std::size_t>(bytes_read));
                chunk.Clear();
                chunk.set_file_name(file.name);
                chunk.set_data(buffer);
                chunk.set_offset(offset);
                chunk.set_raft_term(meta_copy.raft_term);
                chunk.set_raft_index(meta_copy.raft_index);
                chunk.set_dim(meta_copy.dim);
                chunk.set_metric(meta_copy.metric);
                chunk.set_data_type(meta_copy.data_type);

                const bool file_finished = file.stream->peek() == std::char_traits<char>::eof();
                chunk.set_is_last(file_finished && file_index + 1 == files.size());
                if (chunk.is_last()) {
                    emitted_terminal_chunk = true;
                }
                if (file_finished) {
                    ++file_index;
                }
                return Result<bool>::Ok(true);
            }

            return Result<bool>::Ok(false);
        });
    if (!response) {
        UpdatePeerFailure(peer_id);
        return Result<void>::Err(response.error);
    }

    if (response->term() > CurrentTerm()) {
        auto become_follower = BecomeFollower(response->term());
        if (!become_follower) {
            return become_follower;
        }
        return Result<void>::Ok();
    }

    if (!response->success()) {
        return Result<void>::Err("发送快照失败: 接收方返回 success=false");
    }

    const auto sent_at = std::chrono::steady_clock::now();
    UpdatePeerAck(peer_id, meta->raft_index, sent_at);
    return Result<void>::Ok();
}

Result<raftvdb::proto::TopologyInfo> RaftNode::BuildTopologyForPeer(const std::string& peer_id) const {
    return topology_->ToProto(peer_id);
}

void RaftNode::ResetPeerProgressLocked(uint64_t next_index) {
    std::unique_lock progress_lock(peer_progress_mutex_);
    peer_progress_.clear();
    const auto now = std::chrono::steady_clock::now();
    for (const auto& peer : config_.cluster.peers) {
        if (peer.empty() || peer == self_id_) {
            continue;
        }

        PeerProgress progress;
        progress.peer_id = peer;
        progress.next_index = next_index;
        progress.match_index = 0;
        progress.in_flight = 0;
        progress.healthy = true;
        progress.direct_mode = false;
        progress.effective_window_size = config_.raft.pipeline_window_size;
        progress.last_ack_time = now;
        progress.last_send_time = now;
        progress.recover_deadline = now;
        ResetPeerRollbackStateLocked(progress);
        peer_progress_[peer] = progress;
    }
}

void RaftNode::ResetPeerRollbackStateLocked(PeerProgress& progress) {
    progress.rollback_anchor_index = 0;
    progress.rollback_failures = 0;
}

void RaftNode::ApplyPeerRollbackStateLocked(PeerProgress& progress,
                                            uint64_t attempted_next_index,
                                            uint64_t conflict_index) {
    const uint32_t linear_threshold =
        std::max<uint32_t>(1U, config_.raft.pipeline_window_size * 2U);

    if (progress.rollback_anchor_index == 0) {
        progress.rollback_anchor_index = attempted_next_index;
    }
    if (conflict_index != 0 && conflict_index < progress.rollback_anchor_index) {
        progress.rollback_anchor_index = conflict_index;
    }

    progress.rollback_failures += 1U;
    uint64_t retreat = progress.rollback_failures;
    if (progress.rollback_failures > linear_threshold) {
        const uint32_t exponential_rounds = progress.rollback_failures - linear_threshold;
        const uint32_t capped_rounds = std::min<uint32_t>(exponential_rounds - 1U, 20U);
        retreat = static_cast<uint64_t>(linear_threshold) + (1ULL << capped_rounds);
    }

    progress.next_index = progress.rollback_anchor_index > retreat
                              ? progress.rollback_anchor_index - retreat
                              : 1U;
}

void RaftNode::UpdatePeerAck(const std::string& peer_id,
                             uint64_t match_index,
                             std::chrono::steady_clock::time_point sent_at) {
    const auto now = std::chrono::steady_clock::now();
    topology_->MarkHealthy(peer_id);
    std::unique_lock progress_lock(peer_progress_mutex_);
    auto& progress = peer_progress_[peer_id];
    progress.peer_id = peer_id;
    progress.match_index = std::max(progress.match_index, match_index);
    progress.next_index = std::max(progress.next_index, match_index + 1);
    if (progress.in_flight > 0) {
        progress.in_flight -= 1;
    }
    progress.healthy = true;
    progress.last_ack_time = now;
    ResetPeerRollbackStateLocked(progress);

    const double sample_ms =
        static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(now - sent_at).count());
    progress.avg_ack_latency_ms =
        ((progress.avg_ack_latency_ms * static_cast<double>(progress.ack_samples)) + sample_ms) /
        static_cast<double>(progress.ack_samples + 1U);
    progress.ack_samples += 1;
    if (match_index >= LogicalLastIndex() &&
        (progress.direct_mode ||
         progress.effective_window_size != config_.raft.pipeline_window_size)) {
        topology_refresh_requested_ = true;
    }
}

void RaftNode::UpdatePeerHeartbeatAck(const std::string& peer_id,
                                      std::chrono::steady_clock::time_point sent_at) {
    const auto now = std::chrono::steady_clock::now();
    topology_->MarkHealthy(peer_id);
    std::unique_lock progress_lock(peer_progress_mutex_);
    auto& progress = peer_progress_[peer_id];
    progress.peer_id = peer_id;
    progress.healthy = true;
    progress.last_ack_time = now;

    const double sample_ms =
        static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(now - sent_at).count());
    progress.avg_ack_latency_ms =
        ((progress.avg_ack_latency_ms * static_cast<double>(progress.ack_samples)) + sample_ms) /
        static_cast<double>(progress.ack_samples + 1U);
    progress.ack_samples += 1;
}

void RaftNode::UpdatePeerFailure(const std::string& peer_id, uint64_t restore_next_index) {
    std::unique_lock progress_lock(peer_progress_mutex_);
    auto& progress = peer_progress_[peer_id];
    progress.peer_id = peer_id;
    if (progress.in_flight > 0) {
        progress.in_flight -= 1;
    }
    if (restore_next_index > 0) {
        progress.next_index = std::min(progress.next_index, restore_next_index);
    }
}

void RaftNode::RequestReplication() {
    {
        std::lock_guard lock(replicate_mutex_);
        replicate_requested_ = true;
    }
    replicate_cv_.notify_one();
}

void RaftNode::RequestImmediateHeartbeat() {
    {
        std::lock_guard lock(heartbeat_mutex_);
        heartbeat_requested_ = true;
    }
    heartbeat_cv_.notify_one();
}

void RaftNode::ApplyCommittedEntries() {
    while (applied_index_.load(std::memory_order_acquire) < commit_index_.load(std::memory_order_acquire)) {
        const uint64_t next_index = applied_index_.load(std::memory_order_acquire) + 1;
        auto entry = wal_->Read(next_index);
        if (!entry) {
            LOG_ERROR("APPLY_READ_FAILED", "node_id={}, index={}, error={}", self_id_, next_index,
                      entry.error);
            return;
        }

        bool success = true;
        std::string error;
        std::string request_id;

        if (entry->type == EntryType::kNormal) {
            std::shared_ptr<VectorIndex> vector_index;
            {
                std::shared_lock index_lock(vector_index_mutex_);
                vector_index = vector_index_;
            }
            if (!vector_index) {
                success = false;
                error = "向量索引尚未初始化";
            } else if (entry->cmd_type == CmdType::kUpsert) {
                auto command = UpsertCmd::Deserialize(entry->payload);
                if (!command) {
                    success = false;
                    error = command.error;
                } else {
                    request_id = command->request_id;
                    auto upsert = vector_index->Upsert(command->id, command->vector.data(),
                                                       command->vector.size());
                    success = static_cast<bool>(upsert);
                    if (!success) {
                        error = upsert.error;
                    }
                }
            } else if (entry->cmd_type == CmdType::kDelete) {
                auto command = DeleteCmd::Deserialize(entry->payload);
                if (!command) {
                    success = false;
                    error = command.error;
                } else {
                    request_id = command->request_id;
                    auto deleted = vector_index->Delete(command->id);
                    success = static_cast<bool>(deleted);
                    if (!success) {
                        error = deleted.error;
                    }
                }
            }
        }

        if (!request_id.empty()) {
            dedup_table_->Record(request_id, next_index, success, error);
        }

        applied_index_.store(next_index, std::memory_order_release);
        MaybeTriggerSnapshot(next_index);
    }
}

void RaftNode::MaybeTriggerSnapshot(uint64_t applied_index) {
    if (config_.raft.snapshot_threshold == 0U) {
        return;
    }

    const uint64_t last_snapshot_index = last_snapshot_index_.load(std::memory_order_acquire);
    if (applied_index <= last_snapshot_index) {
        return;
    }
    if (applied_index - last_snapshot_index <= config_.raft.snapshot_threshold) {
        return;
    }

    bool expected = false;
    if (!snapshot_in_progress_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }

    LOG_INFO("SNAPSHOT_TRIGGERED", "node_id={}, applied_index={}, last_snapshot_index={}", self_id_,
             applied_index, last_snapshot_index);
    LaunchSnapshotTask(applied_index);
}

void RaftNode::LaunchSnapshotTask(uint64_t snapshot_index) {
    std::shared_ptr<VectorIndex> vector_index;
    {
        std::shared_lock index_lock(vector_index_mutex_);
        vector_index = vector_index_;
    }
    if (!vector_index) {
        snapshot_in_progress_.store(false, std::memory_order_release);
        LOG_ERROR("SNAPSHOT_START_FAILED", "node_id={}, index={}, error=向量索引尚未初始化", self_id_,
                  snapshot_index);
        return;
    }

    const auto dedup_table = dedup_table_;
    const auto storage_config = config_.storage;
    const auto vector_config = config_.vector;
    const auto node_id = self_id_;
    const auto weak_self = weak_from_this();

    try {
        std::thread([weak_self, vector_index = std::move(vector_index), dedup_table,
                     storage_config, vector_config, node_id, snapshot_index]() {
            auto finalize = [&weak_self]() {
                if (auto self = weak_self.lock()) {
                    self->snapshot_in_progress_.store(false, std::memory_order_release);
                }
            };

            SnapshotStore store(storage_config.snapshot_dir);
            auto init_store = store.Initialize();
            if (!init_store) {
                LOG_ERROR("SNAPSHOT_INIT_FAILED", "node_id={}, index={}, error={}", node_id,
                          snapshot_index, init_store.error);
                finalize();
                return;
            }

            auto save_snapshot = vector_index->SaveSnapshot(store.TemporarySnapshotPath());
            if (!save_snapshot) {
                LOG_ERROR("SNAPSHOT_SAVE_FAILED", "node_id={}, index={}, error={}", node_id,
                          snapshot_index, save_snapshot.error);
                finalize();
                return;
            }

            auto self = weak_self.lock();
            if (!self) {
                return;
            }

            auto snapshot_term = self->wal_->TermAt(snapshot_index);
            if (!snapshot_term) {
                LOG_ERROR("SNAPSHOT_META_FAILED", "node_id={}, index={}, error={}", node_id,
                          snapshot_index, snapshot_term.error);
                self->snapshot_in_progress_.store(false, std::memory_order_release);
                return;
            }

            SnapshotMeta meta;
            meta.raft_term = *snapshot_term;
            meta.raft_index = snapshot_index;
            meta.dim = vector_config.dim;
            meta.metric = vector_config.metric;
            meta.data_type = vector_config.data_type;
            meta.created_at = BuildSnapshotCreatedAt();

            auto finalize_snapshot = store.FinalizeSnapshot(meta);
            if (!finalize_snapshot) {
                LOG_ERROR("SNAPSHOT_COMMIT_FAILED", "node_id={}, index={}, error={}", node_id,
                          snapshot_index, finalize_snapshot.error);
                self->snapshot_in_progress_.store(false, std::memory_order_release);
                return;
            }

            self->last_snapshot_index_.store(snapshot_index, std::memory_order_release);
            self->last_snapshot_term_.store(*snapshot_term, std::memory_order_release);

            auto truncate = self->wal_->TruncateBefore(snapshot_index);
            if (!truncate) {
                LOG_ERROR("SNAPSHOT_TRUNCATE_WAL_FAILED", "node_id={}, index={}, error={}", node_id,
                          snapshot_index, truncate.error);
            }

            if (dedup_table) {
                auto save_dedup = dedup_table->SaveTo(DedupSnapshotPath(storage_config));
                if (!save_dedup) {
                    LOG_ERROR("SNAPSHOT_SAVE_DEDUP_FAILED", "node_id={}, index={}, error={}",
                              node_id, snapshot_index, save_dedup.error);
                }
            }

            LOG_INFO("SNAPSHOT_COMPLETED", "node_id={}, index={}", node_id, snapshot_index);
            self->snapshot_in_progress_.store(false, std::memory_order_release);
        }).detach();
    } catch (const std::system_error& error) {
        snapshot_in_progress_.store(false, std::memory_order_release);
        LOG_ERROR("SNAPSHOT_THREAD_FAILED", "node_id={}, index={}, error={}", self_id_,
                  snapshot_index, error.what());
    }
}

Result<void> RaftNode::ApplyInstalledSnapshot(const SnapshotMeta& snapshot_meta) {
    auto loaded_index =
        VectorIndex::LoadFromSnapshot(SnapshotStore(config_.storage.snapshot_dir).SnapshotPath(),
                                      config_.vector);
    if (!loaded_index) {
        return Result<void>::Err(loaded_index.error);
    }

    {
        std::unique_lock index_lock(vector_index_mutex_);
        vector_index_ = *loaded_index;
    }

    auto truncate = wal_->TruncateBefore(snapshot_meta.raft_index + 1U);
    if (!truncate) {
        return truncate;
    }

    applied_index_.store(snapshot_meta.raft_index, std::memory_order_release);
    commit_index_.store(snapshot_meta.raft_index, std::memory_order_release);
    last_snapshot_index_.store(snapshot_meta.raft_index, std::memory_order_release);
    last_snapshot_term_.store(snapshot_meta.raft_term, std::memory_order_release);

    auto load_dedup = dedup_table_->LoadFrom(DedupSnapshotPath(config_.storage));
    if (!load_dedup) {
        return load_dedup;
    }

    apply_cv_.notify_all();
    return Result<void>::Ok();
}

Result<void> RaftNode::EnsureLeaseReadable() {
    if (!IsLeader()) {
        return Result<void>::Err("当前节点不是 Leader，无法执行租约读");
    }

    if (!lease_->IsValid()) {
        auto refresh = BroadcastHeartbeat(false);
        if (!refresh) {
            return refresh;
        }
        if (!lease_->IsValid()) {
            return Result<void>::Err("租约无效，心跳续约未成功");
        }
    }

    if (applied_index_.load(std::memory_order_acquire) <
        commit_index_.load(std::memory_order_acquire)) {
        std::unique_lock lock(apply_mutex_);
        apply_cv_.wait_for(lock, std::chrono::milliseconds(500), [this]() {
            return applied_index_.load(std::memory_order_acquire) >=
                   commit_index_.load(std::memory_order_acquire);
        });
    }

    if (applied_index_.load(std::memory_order_acquire) <
        commit_index_.load(std::memory_order_acquire)) {
        return Result<void>::Err("状态机尚未追上 commit_index");
    }
    return Result<void>::Ok();
}

void RaftNode::CheckMentorTimeouts() {
    if (!IsLeader()) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    const auto mentor_timeout = std::chrono::milliseconds(config_.raft.mentor_ack_timeout_ms);

    for (const auto& mentor : topology_->GetMentors()) {
        PeerProgress progress;
        {
            std::shared_lock progress_lock(peer_progress_mutex_);
            auto found = peer_progress_.find(mentor.node_id);
            if (found == peer_progress_.end()) {
                continue;
            }
            progress = found->second;
        }

        if (now - progress.last_ack_time <= mentor_timeout) {
            continue;
        }

        LOG_WARN("MENTOR_TIMEOUT", "leader_id={}, mentor_id={}", self_id_, mentor.node_id);
        auto follower = topology_->GetFollowerOf(mentor.node_id);
        topology_->MarkUnhealthy(mentor.node_id);
        UpdatePeerFailure(mentor.node_id);

        if (follower && follower->healthy) {
            raftvdb::proto::HeartbeatRequest request;
            request.set_term(CurrentTerm());
            request.set_leader_id(self_id_);
            request.set_commit_index(CommitIndex());
            auto topo = BuildTopologyForPeer(follower->node_id);
            if (topo) {
                *request.mutable_topology() = *topo;
            }

            auto sent_at = std::chrono::steady_clock::now();
            auto response = raft_client_->Heartbeat(ResolvePeerAddress(follower->node_id), request);
            if (response && response->success()) {
                topology_->PromoteToMentor(follower->node_id);
                UpdatePeerHeartbeatAck(follower->node_id, sent_at);
                topology_refresh_requested_ = true;
                RequestImmediateHeartbeat();
                continue;
            }

            topology_->MarkUnhealthy(follower->node_id);
        }

        if (HealthyPeerIds().size() + 1U < QuorumSize()) {
            LOG_WARN("CLUSTER_DEGRADED", "leader_id={}, quorum={}, healthy={}", self_id_,
                     QuorumSize(), HealthyPeerIds().size() + 1U);
        }
    }
}

void RaftNode::CheckFollowerTimeouts() {
    if (!IsLeader()) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    const auto follower_timeout =
        std::chrono::milliseconds(config_.raft.mentor_ack_timeout_ms * 2U);

    for (const auto& node : topology_->AllNodes()) {
        if (node.node_id == self_id_ || node.role != NodeRole::kFollower || !node.healthy) {
            continue;
        }

        PeerProgress follower_progress;
        {
            std::shared_lock progress_lock(peer_progress_mutex_);
            auto found = peer_progress_.find(node.node_id);
            if (found == peer_progress_.end()) {
                continue;
            }
            follower_progress = found->second;
        }

        if (now - follower_progress.last_ack_time <= follower_timeout) {
            continue;
        }

        const auto mentor = topology_->GetMentorOf(node.node_id);
        bool mentor_recent = false;
        std::string mentor_id;
        if (mentor && mentor->healthy) {
            mentor_id = mentor->node_id;
            std::shared_lock progress_lock(peer_progress_mutex_);
            auto found = peer_progress_.find(mentor_id);
            if (found != peer_progress_.end()) {
                mentor_recent = now - found->second.last_ack_time <=
                                std::chrono::milliseconds(config_.raft.mentor_ack_timeout_ms);
            }
        }

        // 这条日志用于观测“配置驱动的 Follower 超时检测”何时触发。
        // T-41 集成测试会基于它比较不同 mentor_ack_timeout_ms 下的检测时延。
        LOG_WARN("FOLLOWER_TIMEOUT", "leader_id={}, follower_id={}, mentor_id={}, mentor_recent={}",
                 self_id_, node.node_id, mentor_id, mentor_recent);

        bool follower_ok = false;
        const auto sent_at = std::chrono::steady_clock::now();
        if (follower_progress.next_index <= LogicalLastIndex()) {
            raftvdb::proto::AppendEntriesRequest request;
            request.set_term(CurrentTerm());
            request.set_leader_id(self_id_);
            request.set_prev_log_index(follower_progress.next_index - 1);
            auto prev_term = TermAtLogicalIndex(follower_progress.next_index - 1U);
            if (prev_term) {
                request.set_prev_log_term(*prev_term);
            }
            request.set_leader_commit(CommitIndex());
            auto topo = BuildTopologyForPeer(node.node_id);
            if (topo) {
                *request.mutable_topology() = *topo;
            }

            auto entries = wal_->ReadFrom(follower_progress.next_index, config_.raft.batch_max_entries);
            if (entries) {
                for (const auto& entry : *entries) {
                    auto* proto_entry = request.add_entries();
                    proto_entry->set_index(entry.index);
                    proto_entry->set_term(entry.term);
                    proto_entry->set_type(static_cast<uint32_t>(entry.type));
                    proto_entry->set_cmd_type(static_cast<uint32_t>(entry.cmd_type));
                    proto_entry->set_payload(entry.payload);
                }
            }

            auto response = raft_client_->AppendEntries(ResolvePeerAddress(node.node_id), request);
            if (response && response->success()) {
                follower_ok = true;
                UpdatePeerAck(node.node_id, response->match_index(), sent_at);
            }
        } else {
            raftvdb::proto::HeartbeatRequest request;
            request.set_term(CurrentTerm());
            request.set_leader_id(self_id_);
            request.set_commit_index(CommitIndex());
            auto topo = BuildTopologyForPeer(node.node_id);
            if (topo) {
                *request.mutable_topology() = *topo;
            }

            auto response = raft_client_->Heartbeat(ResolvePeerAddress(node.node_id), request);
            if (response && response->success()) {
                follower_ok = true;
                UpdatePeerHeartbeatAck(node.node_id, sent_at);
            }
        }

        if (follower_ok) {
            if (!mentor_recent) {
                topology_->PromoteToMentor(node.node_id);
                if (!mentor_id.empty()) {
                    topology_->MarkUnhealthy(mentor_id);
                }
                topology_refresh_requested_ = true;
                continue;
            }

            std::unique_lock progress_lock(peer_progress_mutex_);
            auto mentor_it = peer_progress_.find(mentor_id);
            auto follower_it = peer_progress_.find(node.node_id);
            if (mentor_it == peer_progress_.end() || follower_it == peer_progress_.end()) {
                continue;
            }

            // 低样本或高延迟时，优先按“处理能力受限”处理：降低 Mentor 窗口，
            // 同时允许 Leader 临时直发该 Follower。
            if (mentor_it->second.ack_samples < 3 ||
                mentor_it->second.avg_ack_latency_ms >
                    static_cast<double>(config_.raft.heartbeat_interval_ms * 2U) ||
                mentor_it->second.match_index + 1 < LogicalLastIndex()) {
                mentor_it->second.effective_window_size =
                    std::max<uint32_t>(1U, config_.raft.pipeline_window_size / 2U);
                mentor_it->second.recover_deadline =
                    now + std::chrono::milliseconds(config_.raft.mentor_recover_timeout_ms);
                follower_it->second.direct_mode = true;
            } else {
                // 更像链路阻塞时，优先尝试交换绑定；若没有可交换 Mentor，则退化为直发。
                auto mentors = topology_->GetMentors();
                auto other = std::find_if(mentors.begin(), mentors.end(), [&](const NodeInfo& info) {
                    return info.node_id != mentor_id && info.healthy;
                });
                if (other != mentors.end()) {
                    topology_->SwapFollowers(mentor_id, other->node_id);
                } else {
                    follower_it->second.direct_mode = true;
                }
                topology_refresh_requested_ = true;
            }
            continue;
        }

        if (mentor_recent) {
            topology_->MarkUnhealthy(node.node_id);
            LOG_INFO("FOLLOWER_REMOVED_FROM_TOPOLOGY",
                     "leader_id={}, follower_id={}, mentor_id={}", self_id_, node.node_id,
                     mentor_id);
            topology_refresh_requested_ = true;
            RequestImmediateHeartbeat();
        } else if (HealthyPeerIds().size() + 1U < QuorumSize()) {
            LOG_WARN("CLUSTER_DEGRADED", "leader_id={}, quorum={}, healthy={}", self_id_,
                     QuorumSize(), HealthyPeerIds().size() + 1U);
        }
    }
}

Result<void> RaftNode::RebalanceTopology() {
    auto rebalance = topology_->Rebalance(HealthyPeerIds());
    if (!rebalance) {
        return rebalance;
    }

    {
        std::unique_lock progress_lock(peer_progress_mutex_);
        for (auto& [_, progress] : peer_progress_) {
            progress.direct_mode = false;
            progress.effective_window_size = config_.raft.pipeline_window_size;
        }
    }

    last_topology_refresh_ = std::chrono::steady_clock::now();
    topology_refresh_requested_ = false;
    RequestImmediateHeartbeat();
    return Result<void>::Ok();
}

std::vector<std::string> RaftNode::HealthyPeerIds() const {
    std::vector<std::string> healthy;
    for (const auto& node : topology_->AllNodes()) {
        if (node.node_id != self_id_ && node.healthy) {
            healthy.push_back(node.node_id);
        }
    }
    return healthy;
}

void RaftNode::ElectionLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        if (IsLeader()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }

        std::chrono::steady_clock::time_point deadline;
        {
            std::lock_guard state_lock(state_mutex_);
            deadline = election_deadline_;
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            auto start_election = StartElection();
            if (!start_election) {
                LOG_WARN("ELECTION_FAILED", "node_id={}, error={}", self_id_, start_election.error);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void RaftNode::HeartbeatLoop(std::stop_token stop_token) {
    const auto interval = std::chrono::milliseconds(config_.raft.heartbeat_interval_ms);
    while (!stop_token.stop_requested()) {
        std::unique_lock lock(heartbeat_mutex_);
        heartbeat_cv_.wait_for(lock, interval, [this, &stop_token]() {
            return stop_token.stop_requested() || heartbeat_requested_;
        });
        const bool requested = heartbeat_requested_;
        heartbeat_requested_ = false;
        lock.unlock();

        if (stop_token.stop_requested()) {
            return;
        }
        if (!requested && !IsLeader()) {
            continue;
        }
        if (!IsLeader()) {
            continue;
        }

        auto heartbeat = BroadcastHeartbeat(false);
        if (!heartbeat) {
            LOG_WARN("HEARTBEAT_ROUND_FAILED", "node_id={}, error={}", self_id_, heartbeat.error);
        }
    }
}

void RaftNode::ReplicationLoop(std::stop_token stop_token) {
    const auto flush_interval = std::chrono::microseconds(config_.raft.batch_flush_interval_us);
    while (!stop_token.stop_requested()) {
        std::unique_lock lock(replicate_mutex_);
        replicate_cv_.wait_for(lock, flush_interval, [this, &stop_token]() {
            return stop_token.stop_requested() || replicate_requested_;
        });
        replicate_requested_ = false;
        lock.unlock();

        if (stop_token.stop_requested()) {
            return;
        }
        if (!IsLeader()) {
            continue;
        }

        for (const auto& node : topology_->AllNodes()) {
            if (node.node_id == self_id_ || !node.healthy) {
                continue;
            }

            bool should_replicate = node.role == NodeRole::kMentor || node.source_node_id == self_id_;
            uint32_t current_in_flight = 0;
            bool direct_mode = false;
            {
                std::shared_lock progress_lock(peer_progress_mutex_);
                auto found = peer_progress_.find(node.node_id);
                if (found != peer_progress_.end()) {
                    current_in_flight = found->second.in_flight;
                    direct_mode = found->second.direct_mode;
                    if (found->second.direct_mode) {
                        should_replicate = true;
                    }
                }
            }
            if (!should_replicate) {
                continue;
            }

            LOG_DEBUG("REPLICATION_LOOP_PEER",
                      "leader_id={}, peer_id={}, role={}, direct_mode={}, in_flight={}",
                      self_id_,
                      node.node_id,
                      NodeRoleName(node.role),
                      direct_mode,
                      current_in_flight);

            auto replicate = ReplicatePeerOnce(node.node_id);
            if (!replicate) {
                LOG_WARN("REPLICATE_PEER_FAILED", "leader_id={}, peer_id={}, error={}", self_id_,
                         node.node_id, replicate.error);
            }
        }
    }
}

void RaftNode::ApplyLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        std::unique_lock lock(apply_mutex_);
        apply_cv_.wait_for(lock, std::chrono::milliseconds(50), [this, &stop_token]() {
            return stop_token.stop_requested() ||
                   applied_index_.load(std::memory_order_acquire) <
                       commit_index_.load(std::memory_order_acquire);
        });
        lock.unlock();

        if (stop_token.stop_requested()) {
            return;
        }
        ApplyCommittedEntries();
    }
}

void RaftNode::MaintenanceLoop(std::stop_token stop_token) {
    const auto interval = std::chrono::milliseconds(
        std::max<uint32_t>(20U, std::min(config_.raft.heartbeat_interval_ms,
                                         config_.raft.mentor_ack_timeout_ms / 2U)));

    while (!stop_token.stop_requested()) {
        std::this_thread::sleep_for(interval);
        if (stop_token.stop_requested()) {
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        MaybeRunIndexMaintenance(now);

        if (!IsLeader()) {
            continue;
        }

        if (!lease_->IsValid()) {
            RequestImmediateHeartbeat();
        }

        CheckMentorTimeouts();
        CheckFollowerTimeouts();

        {
            std::unique_lock progress_lock(peer_progress_mutex_);
            const auto now = std::chrono::steady_clock::now();
            for (auto& [_, progress] : peer_progress_) {
                if (progress.effective_window_size != config_.raft.pipeline_window_size &&
                    progress.recover_deadline <= now) {
                    progress.effective_window_size = config_.raft.pipeline_window_size;
                    progress.direct_mode = false;
                    topology_refresh_requested_ = true;
                }
            }
        }

        if (topology_refresh_requested_ ||
            now - last_topology_refresh_ >=
                std::chrono::milliseconds(config_.raft.topology_refresh_interval_ms)) {
            auto refresh = RebalanceTopology();
            if (!refresh) {
                LOG_WARN("TOPOLOGY_REFRESH_FAILED", "leader_id={}, error={}", self_id_,
                         refresh.error);
            }
        }
    }
}

void RaftNode::MaybeRunIndexMaintenance(std::chrono::steady_clock::time_point now) {
    if (!index_maintenance_ || !vector_index_) {
        return;
    }

    const auto check_interval = std::chrono::seconds(config_.index_maintenance.check_interval_s);
    if (now - last_index_maintenance_check_ < check_interval) {
        return;
    }
    last_index_maintenance_check_ = now;

    std::shared_ptr<VectorIndex> index;
    {
        std::shared_lock lock(vector_index_mutex_);
        index = vector_index_;
    }
    if (!index) {
        return;
    }

    LOG_DEBUG("INDEX_MAINTENANCE_CHECK",
              "node_id={}, size={}, total_slots={}, deleted={}, state={}",
              self_id_,
              index->Size(),
              index->TotalSlots(),
              index->DeletedCount(),
              static_cast<int>(index_maintenance_->State()));
    index_maintenance_->Check(*index);
}

size_t RaftNode::QuorumSize() const {
    return (config_.cluster.peers.size() + 1U) / 2U + 1U;
}

std::string RaftNode::MetaPath() const {
    return (std::filesystem::path(config_.storage.raft_log_dir) / "meta.bin").string();
}

std::string RaftNode::ResolvePeerAddress(const std::string& peer_id) const {
    auto node = topology_->GetNode(peer_id);
    if (node && !node->addr.empty()) {
        return node->addr;
    }
    return peer_id;
}

void RaftNode::RegisterConfiguredPeers() {
    topology_->SetSelf(self_id_, self_addr_);
    topology_->RegisterNode(self_id_, self_addr_);
    for (const auto& peer : config_.cluster.peers) {
        if (peer.empty() || peer == self_id_) {
            continue;
        }
        // 当前配置里 peers 仍是地址列表，因此这里临时把“node_id”和“addr”
        // 合并为同一个字符串使用。后续若升级为显式映射，只需集中替换此处。
        topology_->RegisterNode(peer, peer);
    }
}
