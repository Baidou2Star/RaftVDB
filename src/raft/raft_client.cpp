#include "raft/raft_client.hpp"

#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>

#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

namespace {

template <typename Response>
Result<Response> MakeGrpcErrorResult(const grpc::Status& status, std::string_view rpc_name) {
    std::string error = "RPC 调用失败: ";
    error += std::string(rpc_name);
    error += ", code=" + std::to_string(static_cast<int>(status.error_code()));
    error += ", message=" + status.error_message();
    return Result<Response>::Err(std::move(error));
}

template <typename Response>
Result<Response> FinishUnaryRpc(grpc::Status status,
                                Response response,
                                std::string_view rpc_name) {
    if (!status.ok()) {
        return MakeGrpcErrorResult<Response>(status, rpc_name);
    }
    return Result<Response>::Ok(std::move(response));
}

void ApplyDeadline(grpc::ClientContext& context, std::chrono::milliseconds timeout) {
    context.set_deadline(std::chrono::system_clock::now() + timeout);
}

} // namespace

struct RaftClient::SharedState {
    explicit SharedState(RaftClientOptions options_in) : options(std::move(options_in)) {}

    RaftClientOptions options;
    mutable std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<raftvdb::proto::RaftService::Stub>> stubs;
    std::atomic<bool> shutting_down{false};
};

RaftClient::RaftClient(RaftClientOptions options)
    : state_(std::make_shared<SharedState>(std::move(options))) {}

RaftClient::~RaftClient() {
    state_->shutting_down.store(true);
}

Result<void> RaftClient::AppendEntriesAsync(
    const std::string& peer_addr,
    const raftvdb::proto::AppendEntriesRequest& request,
    AppendEntriesCallback callback) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return validate;
    }

    auto stub_result = GetOrCreateStub(peer_addr);
    if (!stub_result) {
        return Result<void>::Err(stub_result.error);
    }

    auto shared_state = state_;
    auto stub = *stub_result;
    try {
        std::thread([shared_state, stub, request, callback = std::move(callback)]() mutable {
            grpc::ClientContext context;
            ApplyDeadline(context, shared_state->options.append_entries_timeout);
            raftvdb::proto::AppendEntriesResponse response;
            grpc::Status status = stub->AppendEntries(&context, request, &response);

            if (shared_state->shutting_down.load() || !callback) {
                return;
            }
            callback(FinishUnaryRpc(std::move(status), std::move(response), "AppendEntries"));
        }).detach();
    } catch (const std::system_error& error) {
        return Result<void>::Err("启动 AppendEntries 后台线程失败: " + std::string(error.what()));
    }

    return Result<void>::Ok();
}

Result<raftvdb::proto::RequestVoteResponse> RaftClient::RequestVote(
    const std::string& peer_addr,
    const raftvdb::proto::RequestVoteRequest& request) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return Result<raftvdb::proto::RequestVoteResponse>::Err(validate.error);
    }

    auto stub_result = GetOrCreateStub(peer_addr);
    if (!stub_result) {
        return Result<raftvdb::proto::RequestVoteResponse>::Err(stub_result.error);
    }

    grpc::ClientContext context;
    ApplyDeadline(context, state_->options.request_vote_timeout);
    raftvdb::proto::RequestVoteResponse response;
    grpc::Status status = (*stub_result)->RequestVote(&context, request, &response);
    return FinishUnaryRpc(std::move(status), std::move(response), "RequestVote");
}

Result<void> RaftClient::HeartbeatAsync(const std::string& peer_addr,
                                        const raftvdb::proto::HeartbeatRequest& request,
                                        HeartbeatCallback callback) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return validate;
    }

    auto stub_result = GetOrCreateStub(peer_addr);
    if (!stub_result) {
        return Result<void>::Err(stub_result.error);
    }

    auto shared_state = state_;
    auto stub = *stub_result;
    try {
        std::thread([shared_state, stub, request, callback = std::move(callback)]() mutable {
            grpc::ClientContext context;
            ApplyDeadline(context, shared_state->options.heartbeat_timeout);
            raftvdb::proto::HeartbeatResponse response;
            grpc::Status status = stub->Heartbeat(&context, request, &response);

            if (shared_state->shutting_down.load() || !callback) {
                return;
            }
            callback(FinishUnaryRpc(std::move(status), std::move(response), "Heartbeat"));
        }).detach();
    } catch (const std::system_error& error) {
        return Result<void>::Err("启动 Heartbeat 后台线程失败: " + std::string(error.what()));
    }

    return Result<void>::Ok();
}

Result<raftvdb::proto::InstallSnapshotResponse> RaftClient::InstallSnapshot(
    const std::string& peer_addr,
    const std::vector<raftvdb::proto::SnapshotChunk>& chunks) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(validate.error);
    }

    auto stub_result = GetOrCreateStub(peer_addr);
    if (!stub_result) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(stub_result.error);
    }

    grpc::ClientContext context;
    ApplyDeadline(context, state_->options.install_snapshot_timeout);
    raftvdb::proto::InstallSnapshotResponse response;
    std::unique_ptr<grpc::ClientWriter<raftvdb::proto::SnapshotChunk>> writer =
        (*stub_result)->InstallSnapshot(&context, &response);
    if (!writer) {
        return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
            "创建 InstallSnapshot 流式写入器失败");
    }

    for (const auto& chunk : chunks) {
        if (!writer->Write(chunk)) {
            writer->WritesDone();
            grpc::Status status = writer->Finish();
            if (!status.ok()) {
                return MakeGrpcErrorResult<raftvdb::proto::InstallSnapshotResponse>(
                    status, "InstallSnapshot");
            }
            return Result<raftvdb::proto::InstallSnapshotResponse>::Err(
                "InstallSnapshot 写入中断，接收方提前关闭流");
        }
    }

    writer->WritesDone();
    grpc::Status status = writer->Finish();
    return FinishUnaryRpc(std::move(status), std::move(response), "InstallSnapshot");
}

Result<raftvdb::proto::LeaderInfo> RaftClient::GetLeader(const std::string& peer_addr) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return Result<raftvdb::proto::LeaderInfo>::Err(validate.error);
    }

    auto stub_result = GetOrCreateStub(peer_addr);
    if (!stub_result) {
        return Result<raftvdb::proto::LeaderInfo>::Err(stub_result.error);
    }

    grpc::ClientContext context;
    ApplyDeadline(context, state_->options.get_leader_timeout);
    raftvdb::proto::Empty request;
    raftvdb::proto::LeaderInfo response;
    grpc::Status status = (*stub_result)->GetLeader(&context, request, &response);
    return FinishUnaryRpc(std::move(status), std::move(response), "GetLeader");
}

size_t RaftClient::CachedPeerCount() const {
    std::lock_guard lock(state_->mutex);
    return state_->stubs.size();
}

Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>> RaftClient::GetOrCreateStub(
    const std::string& peer_addr) const {
    auto validate = ValidatePeerAddress(peer_addr);
    if (!validate) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Err(validate.error);
    }

    std::lock_guard lock(state_->mutex);
    auto found = state_->stubs.find(peer_addr);
    if (found != state_->stubs.end()) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Ok(found->second);
    }

    // 每个 peer 只创建一个 Stub，底层 Channel 会被 gRPC 复用。
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
    if (!channel) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Err(
            "创建 gRPC Channel 失败: " + peer_addr);
    }

    std::shared_ptr<raftvdb::proto::RaftService::Stub> stub(
        raftvdb::proto::RaftService::NewStub(channel).release());
    state_->stubs.emplace(peer_addr, stub);
    return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Ok(std::move(stub));
}

Result<void> RaftClient::ValidatePeerAddress(const std::string& peer_addr) {
    if (peer_addr.empty()) {
        return Result<void>::Err("peer 地址不能为空");
    }
    return Result<void>::Ok();
}
