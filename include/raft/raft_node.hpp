#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
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
#include "storage/snapshot_store.hpp"
#include "storage/wal.hpp"
#include "vector/index_maintenance.hpp"
#include "vector/vector_index.hpp"

// Raft 状态机的三种基础角色。
// 这一层只描述本节点在选举语义中的身份，不等同于链式复制拓扑里的
// leader / mentor / follower 三种数据转发角色。
enum class RaftState { kFollower, kCandidate, kLeader };

// Leader 为每个下游节点维护的复制状态。
// 当前实现会同时把“复制进度”“超时诊断状态”“Pipeline 限流状态”放在一起，
// 这样 T-22 ~ T-28 的逻辑都能围绕这一张表完成。
struct PeerProgress {
    std::string peer_id;
    uint64_t next_index = 1;
    uint64_t match_index = 0;
    uint32_t in_flight = 0;
    bool healthy = true;

    // direct_mode=true 表示当前临时绕过 Mentor，Leader 直接向该节点发日志。
    bool direct_mode = false;

    // 当检测到 Mentor 疑似处理能力不足时，会临时缩小窗口。
    uint32_t effective_window_size = 0;

    // 最近一次收到成功响应的时间。
    std::chrono::steady_clock::time_point last_ack_time{};

    // 最近一次发送复制请求的时间，可用于估算 ACK 延迟。
    std::chrono::steady_clock::time_point last_send_time{};

    // Mentor 进入“降速恢复观察期”时的截止时间。
    std::chrono::steady_clock::time_point recover_deadline{};

    // 用简单滑动平均保存 ACK 延迟样本，供 T-27 的诊断逻辑使用。
    double avg_ack_latency_ms = 0.0;
    uint32_t ack_samples = 0;

    // 日志冲突追赶状态：
    // 1. rollback_anchor_index 记录本轮回溯的起点
    // 2. rollback_failures 记录连续回溯失败次数
    // Leader 会先线性回退，再切换到指数回退。
    uint64_t rollback_anchor_index = 0;
    uint32_t rollback_failures = 0;
};

// 启动 RaftNode 时，允许外部补充一些当前 Config 尚未直接表达的依赖。
struct RaftNodeOptions {
    // 当前节点对外监听地址。
    // 若为空，则很多地方会保守回退为 self_id。
    std::string self_addr;

    // 可选依赖：若外部未传入，Create() 会按配置自动创建默认实例。
    std::shared_ptr<VectorIndex> vector_index;
    std::shared_ptr<DedupTable> dedup_table;
    std::shared_ptr<RaftClient> raft_client;

    // 若启动前已经从 snapshot.meta 恢复了快照元数据，
    // 需要把 lastSnapshotIndex / lastSnapshotTerm 一并注入 RaftNode。
    // 否则节点无法正确理解“被快照截断的日志前缀”。
    uint64_t restored_snapshot_index = 0;
    uint64_t restored_snapshot_term = 0;

    // 是否在 Create() 完成后立刻启动后台线程。
    // 默认关闭，便于单测先把 gRPC Server 拉起，再显式 Start()。
    bool auto_start_background_loops = false;
};

// RaftNode 是项目中的核心状态机实现。
//
// 从 T-19 开始，这个类开始真正承接：
// 1. 选举与 RequestVote
// 2. 心跳与租约续约
// 3. AppendEntries、Pipeline 复制、ApplyLoop
// 4. 链式复制超时诊断与拓扑调整
//
// 为了让实现保持可测试、可控，本版本使用 std::jthread 驱动后台循环，
// 每个循环职责清晰，便于单测直接观察状态变化。
class RaftNode final : public RaftRpcHandler, public std::enable_shared_from_this<RaftNode> {
public:
    static Result<std::shared_ptr<RaftNode>> Create(const Config& config,
                                                    RaftNodeOptions options = {});

    ~RaftNode() override;

    // 启动后台循环：
    // - ElectionLoop
    // - HeartbeatLoop
    // - ReplicationLoop
    // - ApplyLoop
    // - MaintenanceLoop
    Result<void> Start();

    // 停止后台循环。该接口是幂等的，重复调用安全。
    void Stop();

    bool IsRunning() const;

    // 写请求入口：仅 Leader 可调用。
    // 当前会先写本地 WAL，再异步触发复制；在单节点模式下会直接提交。
    Result<uint64_t> Propose(const LogEntry& entry);

    // 保留早期零参接口，用于只检查“当前是否可安全做租约读”。
    Result<uint64_t> LeaseRead();

    // 真正的租约读接口：在租约有效且 appliedIndex 追平 commitIndex 后，
    // 直接在本地向量索引上执行搜索。
    Result<std::vector<VectorIndex::SearchResult>> LeaseRead(const float* vec,
                                                             size_t dim,
                                                             size_t top_k);

    // RaftRpcHandler 实现。
    Result<raftvdb::proto::AppendEntriesResponse> HandleAppendEntries(
        const raftvdb::proto::AppendEntriesRequest& request) override;
    Result<raftvdb::proto::RequestVoteResponse> HandleRequestVote(
        const raftvdb::proto::RequestVoteRequest& request) override;
    Result<raftvdb::proto::HeartbeatResponse> HandleHeartbeat(
        const raftvdb::proto::HeartbeatRequest& request) override;
    Result<raftvdb::proto::InstallSnapshotResponse> HandleInstallSnapshot(
        grpc::ServerReader<raftvdb::proto::SnapshotChunk>* reader) override;
    Result<raftvdb::proto::ClientWriteResponse> HandleClientWrite(
        const raftvdb::proto::ClientWriteRequest& request) override;
    Result<raftvdb::proto::ClientSearchResponse> HandleClientSearch(
        const raftvdb::proto::ClientSearchRequest& request) override;
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

    // ─────────────────────────────────────────────────────────────
    // 状态切换与元数据持久化
    // ─────────────────────────────────────────────────────────────
    Result<void> BecomeFollower(uint64_t term, const std::string& leader_id = {});
    Result<void> BecomeCandidate();
    Result<void> BecomeLeader();
    Result<void> PersistMetaLocked(uint64_t term, const std::string& voted_for);

    // ─────────────────────────────────────────────────────────────
    // 选举与心跳
    // ─────────────────────────────────────────────────────────────
    Result<void> StartElection();
    Result<void> BroadcastHeartbeat(bool allow_async_callbacks);
    void ResetElectionDeadlineLocked();
    void NoteLeaderContactLocked();
    uint64_t LogicalLastIndex() const;
    Result<uint64_t> TermAtLogicalIndex(uint64_t index) const;
    uint64_t LastLogTermLocked();
    bool IsCandidateLogUpToDateLocked(uint64_t candidate_last_log_index,
                                      uint64_t candidate_last_log_term);

    // ─────────────────────────────────────────────────────────────
    // 日志复制与提交推进
    // ─────────────────────────────────────────────────────────────
    Result<uint64_t> AppendEntryLocked(const LogEntry& entry);
    Result<uint64_t> AppendNoopEntryLocked();
    Result<void> MaybeCommit();
    Result<void> ReplicatePeerOnce(const std::string& peer_id);
    Result<void> ForwardToFollower(const raftvdb::proto::AppendEntriesRequest& request);
    Result<void> SendSnapshotToPeer(const std::string& peer_id);
    Result<raftvdb::proto::TopologyInfo> BuildTopologyForPeer(const std::string& peer_id) const;
    void ResetPeerProgressLocked(uint64_t next_index);
    void ResetPeerRollbackStateLocked(PeerProgress& progress);
    void ApplyPeerRollbackStateLocked(PeerProgress& progress,
                                      uint64_t attempted_next_index,
                                      uint64_t conflict_index);
    void UpdatePeerAck(const std::string& peer_id,
                       uint64_t match_index,
                       std::chrono::steady_clock::time_point sent_at);
    void UpdatePeerHeartbeatAck(const std::string& peer_id,
                                std::chrono::steady_clock::time_point sent_at);
    void UpdatePeerFailure(const std::string& peer_id, uint64_t restore_next_index = 0);
    void RequestReplication();
    void RequestImmediateHeartbeat();

    // ─────────────────────────────────────────────────────────────
    // ApplyLoop 与租约读
    // ─────────────────────────────────────────────────────────────
    void ApplyCommittedEntries();
    void MaybeTriggerSnapshot(uint64_t applied_index);
    void LaunchSnapshotTask(uint64_t snapshot_index);
    Result<void> ApplyInstalledSnapshot(const SnapshotMeta& snapshot_meta);
    Result<void> EnsureLeaseReadable();
    void MaybeRunIndexMaintenance(std::chrono::steady_clock::time_point now);

    // ─────────────────────────────────────────────────────────────
    // 拓扑与容错
    // ─────────────────────────────────────────────────────────────
    void CheckMentorTimeouts();
    void CheckFollowerTimeouts();
    Result<void> RebalanceTopology();
    std::vector<std::string> HealthyPeerIds() const;

    // ─────────────────────────────────────────────────────────────
    // 后台循环
    // ─────────────────────────────────────────────────────────────
    void ElectionLoop(std::stop_token stop_token);
    void HeartbeatLoop(std::stop_token stop_token);
    void ReplicationLoop(std::stop_token stop_token);
    void ApplyLoop(std::stop_token stop_token);
    void MaintenanceLoop(std::stop_token stop_token);

    // ─────────────────────────────────────────────────────────────
    // 通用辅助
    // ─────────────────────────────────────────────────────────────
    size_t QuorumSize() const;
    std::string MetaPath() const;
    std::string ResolvePeerAddress(const std::string& peer_id) const;
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
    std::chrono::steady_clock::time_point election_deadline_{};
    std::chrono::steady_clock::time_point last_leader_contact_{};
    std::chrono::steady_clock::time_point last_topology_refresh_{};
    std::chrono::steady_clock::time_point last_index_maintenance_check_{};
    std::atomic<uint64_t> last_snapshot_index_{0};
    std::atomic<uint64_t> last_snapshot_term_{0};
    std::atomic<bool> snapshot_in_progress_{false};

    mutable std::shared_mutex peer_progress_mutex_;
    std::unordered_map<std::string, PeerProgress> peer_progress_;
    mutable std::shared_mutex vector_index_mutex_;

    // Apply / Replication / Heartbeat 三类后台循环各自有独立唤醒条件，
    // 避免一个条件变量被多种职责混用后难以测试。
    mutable std::mutex replicate_mutex_;
    std::condition_variable replicate_cv_;
    bool replicate_requested_ = false;

    mutable std::mutex apply_mutex_;
    std::condition_variable apply_cv_;

    mutable std::mutex heartbeat_mutex_;
    std::condition_variable heartbeat_cv_;
    bool heartbeat_requested_ = false;

    mutable std::mutex lifecycle_mutex_;
    std::atomic<bool> running_{false};
    std::atomic<bool> topology_refresh_requested_{false};

    std::jthread election_thread_;
    std::jthread heartbeat_thread_;
    std::jthread replication_thread_;
    std::jthread apply_thread_;
    std::jthread maintenance_thread_;

    std::unique_ptr<WAL> wal_;
    RaftMeta meta_;
    std::unique_ptr<TopologyManager> topology_;
    std::unique_ptr<LeaseManager> lease_;
    std::unique_ptr<IndexMaintenance> index_maintenance_;
    std::shared_ptr<VectorIndex> vector_index_;
    std::shared_ptr<DedupTable> dedup_table_;
    std::shared_ptr<RaftClient> raft_client_;
};
