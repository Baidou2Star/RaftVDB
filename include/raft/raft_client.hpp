#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "common/result.hpp"
#include "raft.grpc.pb.h"

// RaftClient 专门封装节点间 gRPC 调用。
// 当前阶段优先提供简单、稳定且便于单测的接口：
// 1. 同步 RPC 直接返回 Result<T>
// 2. 异步 RPC 使用“后台线程 + 回调”形式，先满足流水线复制场景
// 3. 每个 peer 只缓存一个 Stub，复用底层连接
struct RaftClientOptions {
    // AppendEntries 往往用于日志复制，允许比投票更宽一点的超时。
    std::chrono::milliseconds append_entries_timeout{1000};

    // RequestVote 是选举关键路径，通常需要更快失败。
    std::chrono::milliseconds request_vote_timeout{500};

    // Heartbeat 频率高，单次超时应尽量短。
    std::chrono::milliseconds heartbeat_timeout{300};

    // InstallSnapshot 可能跨多个 chunk，给更长的超时预算。
    std::chrono::milliseconds install_snapshot_timeout{5000};

    // GetLeader 是探测接口，超时保持较短即可。
    std::chrono::milliseconds get_leader_timeout{500};
};

class RaftClient {
public:
    using AppendEntriesCallback =
        std::function<void(Result<raftvdb::proto::AppendEntriesResponse>)>;
    using HeartbeatCallback =
        std::function<void(Result<raftvdb::proto::HeartbeatResponse>)>;

    explicit RaftClient(RaftClientOptions options = {});
    ~RaftClient();

    Result<void> AppendEntriesAsync(const std::string& peer_addr,
                                    const raftvdb::proto::AppendEntriesRequest& request,
                                    AppendEntriesCallback callback) const;

    Result<raftvdb::proto::RequestVoteResponse> RequestVote(
        const std::string& peer_addr,
        const raftvdb::proto::RequestVoteRequest& request) const;

    Result<void> HeartbeatAsync(const std::string& peer_addr,
                                const raftvdb::proto::HeartbeatRequest& request,
                                HeartbeatCallback callback) const;

    Result<raftvdb::proto::InstallSnapshotResponse> InstallSnapshot(
        const std::string& peer_addr,
        const std::vector<raftvdb::proto::SnapshotChunk>& chunks) const;

    Result<raftvdb::proto::LeaderInfo> GetLeader(const std::string& peer_addr) const;

    // 公开缓存数量，便于后续调试连接复用和测试验证。
    size_t CachedPeerCount() const;

private:
    struct SharedState;

    Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>> GetOrCreateStub(
        const std::string& peer_addr) const;

    static Result<void> ValidatePeerAddress(const std::string& peer_addr);

    std::shared_ptr<SharedState> state_;
};
