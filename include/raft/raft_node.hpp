#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/dedup.hpp"
#include "common/config.hpp"
#include "common/result.hpp"
#include "raft/lease.hpp"
#include "raft/log_entry.hpp"
#include "raft/raft_client.hpp"
#include "raft/raft_server.hpp"
#include "raft/topology.hpp"
#include "storage/raft_meta.hpp"
#include "storage/wal.hpp"
#include "vector/vector_index.hpp"

// Raft 状态机的三种基础角色。
// 这一层只描述本节点在选举语义里的角色，不等同于链式复制中的拓扑角色。
enum class RaftState { kFollower, kCandidate, kLeader };

// Leader 为每个 peer 维护的复制进度。
// 当前阶段先把结构和初始化逻辑搭起来，真正的窗口流控和 ACK 推进会在后续任务补齐。
struct PeerProgress {
    std::string peer_id;
    uint64_t next_index = 1;
    uint64_t match_index = 0;
    uint32_t in_flight = 0;
    bool healthy = true;
    std::chrono::steady_clock::time_point last_ack_time{};
};

// RaftNodeOptions 用于把启动时暂时无法从 Config 直接推导的上下文补进来。
struct RaftNodeOptions {
    // 当前节点对外可达地址。
    // 若暂时为空，则后续 LeaderAddr()/GetLeaderInfo() 会保守回退为 self_id。
    std::string self_addr;

    // 后续 ApplyLoop / LeaseRead 会用到的依赖，这里先允许注入，为后续任务留接口。
    std::shared_ptr<VectorIndex> vector_index;
    std::shared_ptr<DedupTable> dedup_table;
    std::shared_ptr<RaftClient> raft_client;
};

// RaftNode 是项目里 Raft 核心状态机的骨架。
// T-18 先完成三件事：
// 1. 启动时把 WAL / RaftMeta / Topology / Lease 等基础依赖接好
// 2. 实现 Follower/Candidate/Leader 三种状态切换
// 3. 让节点已经能够作为 RaftServer 的 handler 运行起来
//
// 真正的选举、日志复制、ApplyLoop 和租约读优化会在后续任务继续填充。
class RaftNode final : public RaftRpcHandler {
public:
    static Result<std::shared_ptr<RaftNode>> Create(const Config& config,
                                                    RaftNodeOptions options = {});

    ~RaftNode() override = default;

    // 外部提交入口。当前阶段先实现“Leader 本地落盘”这一小步，
    // 为 BecomeLeader() 追加 kNoop 和后续 T-22 的 Propose 链路复用。
    Result<uint64_t> Propose(const LogEntry& entry);

    // 当前阶段先实现基础门禁：租约有效且 applied >= commit 时返回 commit_index。
    // 真正的本地搜索和过期后心跳续约逻辑在 T-25 实现。
    Result<uint64_t> LeaseRead();

    // RaftRpcHandler 实现。
    Result<raftvdb::proto::AppendEntriesResponse> HandleAppendEntries(
        const raftvdb::proto::AppendEntriesRequest& request) override;
    Result<raftvdb::proto::RequestVoteResponse> HandleRequestVote(
        const raftvdb::proto::RequestVoteRequest& request) override;
    Result<raftvdb::proto::HeartbeatResponse> HandleHeartbeat(
        const raftvdb::proto::HeartbeatRequest& request) override;
    Result<raftvdb::proto::InstallSnapshotResponse> HandleInstallSnapshot(
        const std::vector<raftvdb::proto::SnapshotChunk>& chunks) override;
    Result<raftvdb::proto::LeaderInfo> GetLeaderInfo() const override;

    // 基础查询接口。
    bool IsLeader() const;
    RaftState State() const;
    uint64_t CurrentTerm() const;
    uint64_t CommitIndex() const;
    uint64_t AppliedIndex() const;
    std::string LeaderId() const;
    std::string LeaderAddr() const;

private:
    friend class RaftNodeTestPeer;

    RaftNode(Config config,
             RaftNodeOptions options,
             std::unique_ptr<WAL> wal,
             RaftMeta meta,
             std::unique_ptr<TopologyManager> topology,
             std::unique_ptr<LeaseManager> lease);

    Result<void> BecomeFollower(uint64_t term, const std::string& leader_id = {});
    Result<void> BecomeCandidate();
    Result<void> BecomeLeader();

    Result<uint64_t> AppendEntryLocked(const LogEntry& entry);
    Result<uint64_t> AppendNoopEntryLocked();
    Result<void> PersistMetaLocked(uint64_t term, const std::string& voted_for);
    void ResetPeerProgressLocked(uint64_t next_index);
    std::string MetaPath() const;
    void RegisterConfiguredPeers();

    mutable std::mutex state_mutex_;
    Config config_;
    std::string self_id_;
    std::string self_addr_;
    std::atomic<RaftState> state_{RaftState::kFollower};
    std::atomic<uint64_t> current_term_{0};
    std::atomic<uint64_t> commit_index_{0};
    std::atomic<uint64_t> applied_index_{0};
    std::string voted_for_;
    std::string leader_id_;

    mutable std::shared_mutex peer_progress_mutex_;
    std::unordered_map<std::string, PeerProgress> peer_progress_;

    std::unique_ptr<WAL> wal_;
    RaftMeta meta_;
    std::unique_ptr<TopologyManager> topology_;
    std::unique_ptr<LeaseManager> lease_;
    std::shared_ptr<VectorIndex> vector_index_;
    std::shared_ptr<DedupTable> dedup_table_;
    std::shared_ptr<RaftClient> raft_client_;
};
