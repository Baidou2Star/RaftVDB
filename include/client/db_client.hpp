#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "common/config.hpp"
#include "common/result.hpp"
#include "raft.grpc.pb.h"

// 客户端侧的简单请求结构。
// 这样可以避免上层直接依赖 protobuf 细节，后续即便协议演进，
// 也能把变化收敛在 DBClient 内部。
struct UpsertRequest {
    uint64_t id = 0;
    std::vector<float> vector;
};

struct DeleteRequest {
    uint64_t id = 0;
};

struct SearchRequest {
    std::vector<float> vector;
    uint32_t top_k = 10;
};

struct SearchHit {
    uint64_t id = 0;
    float distance = 0.0F;
};

// DBClient 负责客户端侧的寻主、连接复用和自动重试。
// 当前阶段已经落地：
// 1. Connect() 并发探测 peers
// 2. leader 地址缓存
// 3. WithRetry() 的统一重试骨架
// 4. GetLeader() 的重新探测逻辑
// 5. ClientWrite / ClientSearch 两条客户端 RPC
class DBClient {
public:
    static Result<std::unique_ptr<DBClient>> Connect(const std::vector<std::string>& peers,
                                                     ClientConfig config = {});

    Result<raftvdb::proto::LeaderInfo> GetLeader();

    Result<void> Upsert(const UpsertRequest& request);
    Result<void> Delete(const DeleteRequest& request);
    Result<std::vector<SearchHit>> Search(const SearchRequest& request);

private:
    DBClient(std::vector<std::string> peers, ClientConfig config, std::string client_id);

    Result<raftvdb::proto::LeaderInfo> ProbeLeaderConcurrently();
    Result<raftvdb::proto::LeaderInfo> QueryLeaderOn(const std::string& peer_addr) const;
    Result<std::shared_ptr<raftvdb::proto::RaftService::Stub>> GetOrCreateStub(
        const std::string& peer_addr) const;

    std::string NextRequestId();
    std::string ChooseRandomPeer(const std::string& exclude_addr = {}) const;

    template <typename Response, typename Fn>
    Result<Response> WithRetry(Fn&& fn);

    std::vector<std::string> peers_;
    ClientConfig config_;

    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string, std::shared_ptr<raftvdb::proto::RaftService::Stub>> stubs_;
    std::string leader_addr_;
    std::string client_id_;
    std::atomic<uint64_t> client_seq_{0};
};

template <typename Response, typename Fn>
Result<Response> DBClient::WithRetry(Fn&& fn) {
    std::string target_addr;
    {
        std::lock_guard lock(mutex_);
        target_addr = leader_addr_;
    }
    if (target_addr.empty()) {
        auto leader = ProbeLeaderConcurrently();
        if (!leader) {
            return Result<Response>::Err(leader.error);
        }
        target_addr = leader->leader_addr();
    }

    for (uint32_t attempt = 0; attempt <= config_.max_retry_count; ++attempt) {
        auto response = fn(target_addr);
        if (response && response->success()) {
            std::lock_guard lock(mutex_);
            leader_addr_ = target_addr;
            return response;
        }

        // transport 失败：当前地址不可用，随机切到其他 peer 并指数退避。
        if (!response) {
            target_addr = ChooseRandomPeer(target_addr);
        } else if (!response->redirect_to().empty()) {
            // 非 Leader 且给出了明确 hint，直接单跳切换。
            target_addr = response->redirect_to();
        } else if (!response->error().empty()) {
            // success=false 且存在业务错误，直接把错误返回给调用方；
            // 只有“正在选举”的场景才允许继续重试。
            return Result<Response>::Err(response->error());
        } else {
            target_addr = ChooseRandomPeer(target_addr);
        }

        if (attempt == config_.max_retry_count) {
            break;
        }

        const uint64_t multiplier = 1ULL << std::min<uint32_t>(attempt, 20U);
        const uint64_t backoff_ms =
            std::min<uint64_t>(static_cast<uint64_t>(config_.retry_base_ms) * multiplier,
                               static_cast<uint64_t>(config_.retry_max_ms));
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }

    return Result<Response>::Err("重试次数超过上限，仍未定位到可用 Leader");
}
