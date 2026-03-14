#pragma once

#include <grpcpp/server.h>

#include <memory>
#include <mutex>
#include <string>

#include "common/result.hpp"
#include "raft.grpc.pb.h"

// RaftServerOptions 用于描述 gRPC 服务端的监听配置。
// 当前阶段我们只开放最核心的监听地址，避免在 RaftNode 尚未接入前
// 过早暴露一批尚未稳定的网络调优参数。
struct RaftServerOptions {
    // 监听地址，支持直接传入 "127.0.0.1:7001" 或 "127.0.0.1:0"。
    // 当端口为 0 时，gRPC 会自动分配一个空闲端口，适合单元测试使用。
    std::string listen_addr{"127.0.0.1:0"};
};

// RaftRpcHandler 是 RaftServer 与后续 RaftNode 之间的适配接口。
// 之所以在 T-14 单独定义这层抽象，而不是直接依赖尚未实现的 RaftNode，
// 是为了让服务端基础设施先独立落地，并通过单测把 RPC 路由行为固定下来。
// 等 T-18 开始实现 RaftNode 时，只需要让 RaftNode 实现本接口即可无缝接入。
class RaftRpcHandler {
public:
    virtual ~RaftRpcHandler() = default;

    // 处理日志复制请求。未来 RaftNode 可以在这里完成 term 校验、
    // prevLog 对齐、WAL 追加和 commitIndex 推进。
    virtual Result<raftvdb::proto::AppendEntriesResponse> HandleAppendEntries(
        const raftvdb::proto::AppendEntriesRequest& request) = 0;

    // 处理投票请求。后续真正的选举逻辑会在 RaftNode 中补齐。
    virtual Result<raftvdb::proto::RequestVoteResponse> HandleRequestVote(
        const raftvdb::proto::RequestVoteRequest& request) = 0;

    // 处理心跳请求。后续会在这里承接 term 更新、选举计时器重置、
    // commitIndex 同步和拓扑广播。
    virtual Result<raftvdb::proto::HeartbeatResponse> HandleHeartbeat(
        const raftvdb::proto::HeartbeatRequest& request) = 0;

    // 处理快照安装请求。
    // T-30 起改为直接把 gRPC 的流式 reader 透传给业务层，
    // 这样处理器可以边读边落盘，避免大快照先在内存中整包聚合。
    virtual Result<raftvdb::proto::InstallSnapshotResponse> HandleInstallSnapshot(
        grpc::ServerReader<raftvdb::proto::SnapshotChunk>* reader) = 0;

    // 处理客户端写请求。若当前节点不是 Leader，会返回 redirect_to
    // 让 DBClient 直接单跳切到新的 Leader。
    virtual Result<raftvdb::proto::ClientWriteResponse> HandleClientWrite(
        const raftvdb::proto::ClientWriteRequest& request) = 0;

    // 返回当前 Leader 信息。Follower 可以返回自己已知的 leader_addr，
    // Leader 则可直接返回自身地址，供客户端做寻主探测。
    virtual Result<raftvdb::proto::LeaderInfo> GetLeaderInfo() const = 0;
};

// RaftServer 负责承载 gRPC Server 的生命周期，并将所有 RPC
// 路由到 RaftRpcHandler。这里不直接实现 Raft 算法，只做网络入口与调度。
class RaftServer {
public:
    explicit RaftServer(std::shared_ptr<RaftRpcHandler> handler,
                        RaftServerOptions options = {});
    ~RaftServer();

    // 启动 gRPC Server。若 listen_addr 使用端口 0，启动成功后可通过
    // BoundAddress() 读取实际监听地址。
    Result<void> Start();

    // 关闭并等待 gRPC Server 退出。该接口是幂等的，重复调用安全。
    void Stop();

    // 查询服务端当前是否处于运行状态。
    bool IsRunning() const;

    // 返回启动成功后真正绑定的监听地址。
    // 如果服务尚未启动，则返回空字符串。
    std::string BoundAddress() const;

    // 返回配置层声明的监听地址，便于日志打印和调试。
    std::string ListenAddress() const;

private:
    class ServiceImpl;

    std::shared_ptr<RaftRpcHandler> handler_;
    RaftServerOptions options_;
    mutable std::mutex mutex_;
    std::unique_ptr<ServiceImpl> service_;
    std::unique_ptr<grpc::Server> server_;
    std::string bound_address_;
};
