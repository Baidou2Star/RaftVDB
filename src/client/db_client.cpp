#include "client/db_client.hpp"

#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <random>
#include <sstream>
#include <utility>

namespace {

Result<void> ValidatePeers(const std::vector<std::string>& peers) {
    if (peers.empty()) {
        return Result<void>::Err("DBClient::Connect 失败: peers 不能为空");
    }

    const bool has_non_empty =
        std::any_of(peers.begin(), peers.end(), [](const std::string& peer) { return !peer.empty(); });
    if (!has_non_empty) {
        return Result<void>::Err("DBClient::Connect 失败: peers 不能全为空");
    }
    return Result<void>::Ok();
}

std::string GenerateClientId() {
    char host_name[256] = {};
    if (::gethostname(host_name, sizeof(host_name) - 1) != 0) {
        std::snprintf(host_name, sizeof(host_name), "unknown-host");
    }

    std::ostringstream stream;
    stream << host_name << "-" << static_cast<long long>(::getpid());
    return stream.str();
}

std::string SerializeFloatVectorBytes(const std::vector<float>& values) {
    if (values.empty()) {
        return {};
    }

    std::string bytes(values.size() * sizeof(float), '\0');
    std::memcpy(bytes.data(), values.data(), bytes.size());
    return bytes;
}

} // namespace

DBClient::DBClient(std::vector<std::string> peers, ClientConfig config, std::string client_id)
    : peers_(std::move(peers)), config_(config), client_id_(std::move(client_id)) {}

Result<std::unique_ptr<DBClient>> DBClient::Connect(const std::vector<std::string>& peers,
                                                    ClientConfig config) {
    auto validate = ValidatePeers(peers);
    if (!validate) {
        return Result<std::unique_ptr<DBClient>>::Err(validate.error);
    }

    auto client =
        std::unique_ptr<DBClient>(new DBClient(peers, config, GenerateClientId()));
    auto leader = client->ProbeLeaderConcurrently();
    if (!leader) {
        return Result<std::unique_ptr<DBClient>>::Err(leader.error);
    }

    {
        std::lock_guard lock(client->mutex_);
        client->leader_addr_ = leader->leader_addr();
    }
    return Result<std::unique_ptr<DBClient>>::Ok(std::move(client));
}

Result<raftvdb::proto::LeaderInfo> DBClient::GetLeader() {
    std::string cached_leader_addr;
    {
        std::lock_guard lock(mutex_);
        cached_leader_addr = leader_addr_;
    }

    if (!cached_leader_addr.empty()) {
        auto leader = QueryLeaderOn(cached_leader_addr);
        if (leader && !leader->leader_addr().empty()) {
            std::lock_guard lock(mutex_);
            leader_addr_ = leader->leader_addr();
            return leader;
        }
    }

    auto refreshed = ProbeLeaderConcurrently();
    if (!refreshed) {
        return refreshed;
    }

    {
        std::lock_guard lock(mutex_);
        leader_addr_ = refreshed->leader_addr();
    }
    return refreshed;
}

Result<void> DBClient::Upsert(const UpsertRequest& request) {
    if (request.vector.empty()) {
        return Result<void>::Err("Upsert 失败: vector 不能为空");
    }

    // request_id 必须在一次“逻辑写入”内保持稳定。
    // 否则发生 redirect / 重试时，Leader 侧的 DedupTable 会把每次重试都当成新请求。
    const std::string request_id = NextRequestId();

    auto result = WithRetry<raftvdb::proto::ClientWriteResponse>(
        [this, &request, request_id](const std::string& target_addr) {
            auto stub = GetOrCreateStub(target_addr);
            if (!stub) {
                return Result<raftvdb::proto::ClientWriteResponse>::Err(stub.error);
            }

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(config_.retry_max_ms));

            raftvdb::proto::ClientWriteRequest rpc_request;
            rpc_request.set_cmd_type(1U);
            rpc_request.set_id(request.id);
            rpc_request.set_vector(SerializeFloatVectorBytes(request.vector));
            rpc_request.set_request_id(request_id);

            raftvdb::proto::ClientWriteResponse rpc_response;
            const grpc::Status status = (*stub)->ClientWrite(&context, rpc_request, &rpc_response);
            if (!status.ok()) {
                return Result<raftvdb::proto::ClientWriteResponse>::Err(
                    "ClientWrite RPC 失败: " + status.error_message());
            }
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(rpc_response));
        });
    if (!result) {
        return Result<void>::Err(result.error);
    }
    if (!result->success()) {
        return Result<void>::Err(result->error().empty() ? "写入未成功完成" : result->error());
    }
    return Result<void>::Ok();
}

Result<void> DBClient::Delete(const DeleteRequest& request) {
    // Delete 与 Upsert 一样，需要把 request_id 固定在逻辑请求粒度，
    // 这样重试不会破坏幂等语义。
    const std::string request_id = NextRequestId();
    auto result = WithRetry<raftvdb::proto::ClientWriteResponse>(
        [this, &request, request_id](const std::string& target_addr) {
            auto stub = GetOrCreateStub(target_addr);
            if (!stub) {
                return Result<raftvdb::proto::ClientWriteResponse>::Err(stub.error);
            }

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(config_.retry_max_ms));

            raftvdb::proto::ClientWriteRequest rpc_request;
            rpc_request.set_cmd_type(2U);
            rpc_request.set_id(request.id);
            rpc_request.set_request_id(request_id);

            raftvdb::proto::ClientWriteResponse rpc_response;
            const grpc::Status status = (*stub)->ClientWrite(&context, rpc_request, &rpc_response);
            if (!status.ok()) {
                return Result<raftvdb::proto::ClientWriteResponse>::Err(
                    "ClientWrite RPC 失败: " + status.error_message());
            }
            return Result<raftvdb::proto::ClientWriteResponse>::Ok(std::move(rpc_response));
        });
    if (!result) {
        return Result<void>::Err(result.error);
    }
    if (!result->success()) {
        return Result<void>::Err(result->error().empty() ? "删除未成功完成" : result->error());
    }
    return Result<void>::Ok();
}

Result<std::vector<SearchHit>> DBClient::Search(const SearchRequest&) {
    return Result<std::vector<SearchHit>>::Err("DBClient::Search 尚未实现");
}

Result<raftvdb::proto::LeaderInfo> DBClient::ProbeLeaderConcurrently() {
    std::mutex result_mutex;
    std::atomic<bool> found{false};
    std::string last_error;
    raftvdb::proto::LeaderInfo selected;
    std::vector<std::thread> threads;
    threads.reserve(peers_.size());

    for (const auto& peer : peers_) {
        if (peer.empty()) {
            continue;
        }

        threads.emplace_back([this, &result_mutex, &found, &last_error, &selected, peer]() {
            if (found.load(std::memory_order_acquire)) {
                return;
            }

            auto leader = QueryLeaderOn(peer);
            if (!leader) {
                std::lock_guard lock(result_mutex);
                if (last_error.empty()) {
                    last_error = leader.error;
                }
                return;
            }

            // 节点正在选举中时 leader_addr 为空，这种情况不视为错误，只是尚未就绪。
            if (leader->leader_addr().empty()) {
                return;
            }

            if (!found.exchange(true, std::memory_order_acq_rel)) {
                std::lock_guard lock(result_mutex);
                selected = *leader;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    if (!selected.leader_addr().empty()) {
        return Result<raftvdb::proto::LeaderInfo>::Ok(std::move(selected));
    }
    if (!last_error.empty()) {
        return Result<raftvdb::proto::LeaderInfo>::Err("未找到可用 Leader: " + last_error);
    }
    return Result<raftvdb::proto::LeaderInfo>::Err("未找到可用 Leader：所有节点仍在选举中");
}

Result<raftvdb::proto::LeaderInfo> DBClient::QueryLeaderOn(const std::string& peer_addr) const {
    auto stub = GetOrCreateStub(peer_addr);
    if (!stub) {
        return Result<raftvdb::proto::LeaderInfo>::Err(stub.error);
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(config_.retry_base_ms +
                                                   std::max<uint32_t>(config_.retry_base_ms, 200U)));
    raftvdb::proto::Empty request;
    raftvdb::proto::LeaderInfo response;
    grpc::Status status = (*stub)->GetLeader(&context, request, &response);
    if (!status.ok()) {
        return Result<raftvdb::proto::LeaderInfo>::Err("GetLeader RPC 失败: " +
                                                       status.error_message());
    }
    return Result<raftvdb::proto::LeaderInfo>::Ok(std::move(response));
}

Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>> DBClient::GetOrCreateStub(
    const std::string& peer_addr) const {
    if (peer_addr.empty()) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Err("peer 地址不能为空");
    }

    std::lock_guard lock(mutex_);
    auto found = stubs_.find(peer_addr);
    if (found != stubs_.end()) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Ok(found->second);
    }

    auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
    if (!channel) {
        return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Err(
            "创建 DBClient gRPC Channel 失败: " + peer_addr);
    }

    std::shared_ptr<raftvdb::proto::RaftService::Stub> stub(
        raftvdb::proto::RaftService::NewStub(channel).release());
    stubs_.emplace(peer_addr, stub);
    return Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>>::Ok(std::move(stub));
}

std::string DBClient::NextRequestId() {
    const uint64_t next_seq = client_seq_.fetch_add(1, std::memory_order_acq_rel) + 1U;
    return client_id_ + "-" + std::to_string(next_seq);
}

std::string DBClient::ChooseRandomPeer(const std::string& exclude_addr) const {
    std::vector<std::string> candidates;
    candidates.reserve(peers_.size());
    for (const auto& peer : peers_) {
        if (!peer.empty() && peer != exclude_addr) {
            candidates.push_back(peer);
        }
    }
    if (candidates.empty()) {
        return exclude_addr;
    }

    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<std::size_t> dist(0, candidates.size() - 1U);
    return candidates[dist(rng)];
}
