#include "raft/raft_server.hpp"

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

template <typename Response>
grpc::Status CopyResultToResponse(Result<Response> result,
                                  Response* response,
                                  std::string_view handler_name) {
    if (!result) {
        std::string error = "RaftServer 处理失败: ";
        error += std::string(handler_name);
        error += ", error=" + result.error;
        return grpc::Status(grpc::StatusCode::INTERNAL, error);
    }

    *response = std::move(*result);
    return grpc::Status::OK;
}

std::string BuildBoundAddress(const std::string& listen_addr, int port) {
    if (port <= 0) {
        return {};
    }

    const std::size_t colon_pos = listen_addr.rfind(':');
    if (colon_pos == std::string::npos) {
        return listen_addr;
    }

    return listen_addr.substr(0, colon_pos + 1) + std::to_string(port);
}

} // namespace

class RaftServer::ServiceImpl final : public raftvdb::proto::RaftService::Service {
public:
    explicit ServiceImpl(std::shared_ptr<RaftRpcHandler> handler) : handler_(std::move(handler)) {}

    grpc::Status AppendEntries(grpc::ServerContext*,
                               const raftvdb::proto::AppendEntriesRequest* request,
                               raftvdb::proto::AppendEntriesResponse* response) override {
        return CopyResultToResponse(handler_->HandleAppendEntries(*request), response,
                                    "HandleAppendEntries");
    }

    grpc::Status RequestVote(grpc::ServerContext*,
                             const raftvdb::proto::RequestVoteRequest* request,
                             raftvdb::proto::RequestVoteResponse* response) override {
        return CopyResultToResponse(handler_->HandleRequestVote(*request), response,
                                    "HandleRequestVote");
    }

    grpc::Status Heartbeat(grpc::ServerContext*,
                           const raftvdb::proto::HeartbeatRequest* request,
                           raftvdb::proto::HeartbeatResponse* response) override {
        return CopyResultToResponse(handler_->HandleHeartbeat(*request), response,
                                    "HandleHeartbeat");
    }

    grpc::Status InstallSnapshot(
        grpc::ServerContext*,
        grpc::ServerReader<raftvdb::proto::SnapshotChunk>* reader,
        raftvdb::proto::InstallSnapshotResponse* response) override {
        // 当前阶段先把整个流聚合为 chunk 列表，再交给上层处理器。
        // 这样可以保持 RaftNode 侧接口稳定，避免在骨架未完成前直接暴露 gRPC Reader。
        std::vector<raftvdb::proto::SnapshotChunk> chunks;
        raftvdb::proto::SnapshotChunk chunk;
        while (reader->Read(&chunk)) {
            chunks.push_back(chunk);
        }

        return CopyResultToResponse(handler_->HandleInstallSnapshot(chunks), response,
                                    "HandleInstallSnapshot");
    }

    grpc::Status GetLeader(grpc::ServerContext*,
                           const raftvdb::proto::Empty*,
                           raftvdb::proto::LeaderInfo* response) override {
        return CopyResultToResponse(handler_->GetLeaderInfo(), response, "GetLeaderInfo");
    }

private:
    std::shared_ptr<RaftRpcHandler> handler_;
};

RaftServer::RaftServer(std::shared_ptr<RaftRpcHandler> handler, RaftServerOptions options)
    : handler_(std::move(handler)), options_(std::move(options)) {}

RaftServer::~RaftServer() {
    Stop();
}

Result<void> RaftServer::Start() {
    std::lock_guard lock(mutex_);

    if (!handler_) {
        return Result<void>::Err("RaftServer 启动失败: handler 不能为空");
    }
    if (server_) {
        return Result<void>::Err("RaftServer 已经处于运行状态");
    }

    service_ = std::make_unique<ServiceImpl>(handler_);

    grpc::ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort(options_.listen_addr, grpc::InsecureServerCredentials(),
                             &selected_port);
    builder.RegisterService(service_.get());

    server_ = builder.BuildAndStart();
    if (!server_) {
        service_.reset();
        return Result<void>::Err("RaftServer 启动失败: gRPC Server 构建失败");
    }
    if (selected_port <= 0) {
        server_->Shutdown();
        server_->Wait();
        server_.reset();
        service_.reset();
        return Result<void>::Err("RaftServer 启动失败: 未能获取有效监听端口");
    }

    bound_address_ = BuildBoundAddress(options_.listen_addr, selected_port);
    return Result<void>::Ok();
}

void RaftServer::Stop() {
    std::unique_ptr<grpc::Server> server_to_stop;
    std::unique_ptr<ServiceImpl> service_to_stop;

    {
        std::lock_guard lock(mutex_);
        if (!server_) {
            return;
        }

        server_to_stop = std::move(server_);
        service_to_stop = std::move(service_);
        bound_address_.clear();
    }

    server_to_stop->Shutdown();
    server_to_stop->Wait();
}

bool RaftServer::IsRunning() const {
    std::lock_guard lock(mutex_);
    return static_cast<bool>(server_);
}

std::string RaftServer::BoundAddress() const {
    std::lock_guard lock(mutex_);
    return bound_address_;
}

std::string RaftServer::ListenAddress() const {
    return options_.listen_addr;
}
