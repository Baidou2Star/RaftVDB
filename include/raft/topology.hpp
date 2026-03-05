#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/result.hpp"
#include "raft.pb.h"

// 节点在链式复制拓扑中的角色。
// 这里严格对齐技术文档中的三种角色定义，不额外引入 “unknown” 状态，
// 以便后续 RaftNode 的状态判断保持简单直接。
enum class NodeRole { kLeader, kMentor, kFollower };

// 拓扑中的单个节点信息。
// 该结构既能承载 Leader 侧维护的全局拓扑，也能承载 Follower 本地接收到的局部视图。
struct NodeInfo {
    // 节点唯一 ID，后续会与 peer_progress、日志 ACK 等模块对齐。
    std::string node_id;

    // 节点网络地址。当前阶段若尚未显式注册地址，则回退为 node_id 占位。
    std::string addr;

    // 当前角色：leader / mentor / follower。
    NodeRole role = NodeRole::kFollower;

    // 上游节点 ID：
    // 1. mentor 的 source 是 leader
    // 2. follower 的 source 优先是 mentor，在降级兜底场景下也可能直接是 leader
    std::string source_node_id;

    // 健康状态。Rebalance 和局部调整都会基于该字段维护活跃拓扑。
    bool healthy = true;
};

// TopologyManager 负责维护链式复制拓扑。
// 当前阶段的设计目标有两个：
// 1. Leader 侧能够维护“全局拓扑”并对健康节点执行 Rebalance。
// 2. 所有节点都能把“自己的角色视图”序列化为 proto，用于 AppendEntries / Heartbeat 广播。
class TopologyManager {
public:
    explicit TopologyManager(std::string self_id = {}, std::string self_addr = {});

    // 配置当前节点身份。后续节点启动时可在构造后补设。
    void SetSelf(std::string self_id, std::string self_addr = {});

    // 设置当前 Leader。Leader 会被固定为 role=leader, source=""。
    void SetLeader(const std::string& leader_id, const std::string& leader_addr = {});

    // 注册节点地址映射。若节点已存在，也会同步刷新其 addr。
    void RegisterNode(const std::string& node_id, const std::string& addr);
    void RegisterNodes(const std::unordered_map<std::string, std::string>& nodes);

    // 根据健康节点列表重新分配拓扑。
    // 输入列表默认不含 Leader 自身；实现会自动把 Leader 作为固定根节点保留。
    // 为了满足“每个 Mentor 至多挂 1 个 Follower”，mentor 数量采用：
    // mentor_count = floor(total_cluster_size / 2)
    //            = floor((healthy_nodes.size() + 1) / 2)
    Result<void> Rebalance(const std::vector<std::string>& healthy_nodes);

    // 查询接口。
    std::vector<NodeInfo> GetMentors() const;
    std::optional<NodeInfo> GetFollowerOf(const std::string& mentor_id) const;
    std::optional<NodeInfo> GetMentorOf(const std::string& follower_id) const;
    std::optional<NodeInfo> GetNode(const std::string& node_id) const;
    std::vector<NodeInfo> AllNodes() const;

    // 若节点不存在，则 GetRole 保守回退为 follower，GetSource 返回空字符串。
    // 这样调用侧不会因为缺失节点直接崩掉，但测试和上层逻辑仍可通过 GetNode 做显式检查。
    NodeRole GetRole(const std::string& node_id) const;
    std::string GetSource(const std::string& node_id) const;

    // 局部调整接口。
    // 这些操作会尽量保持拓扑仍然可用：若某个 Mentor 被降级或失活，
    // 它原本挂接的 Follower 会被重新挂到其他 Mentor，若没有可用 Mentor，则直接回退到 Leader。
    void PromoteToMentor(const std::string& node_id);
    void DemoteToFollower(const std::string& node_id);
    void SwapFollowers(const std::string& mentor_a, const std::string& mentor_b);
    void MarkUnhealthy(const std::string& node_id);
    void MarkHealthy(const std::string& node_id);

    // 序列化接口。
    // 无参 ToProto/FromProto 作用于当前 self 节点，便于后续在节点本地直接使用。
    raftvdb::proto::TopologyInfo ToProto() const;
    Result<raftvdb::proto::TopologyInfo> ToProto(const std::string& node_id) const;
    Result<void> FromProto(const raftvdb::proto::TopologyInfo& proto);

    // 有参版本可让 Leader 为任意目标节点导出视图，也可让测试直接恢复指定节点状态。
    Result<void> FromProto(const std::string& node_id,
                           const raftvdb::proto::TopologyInfo& proto,
                           const std::string& node_addr = {});

private:
    static std::string RoleToString(NodeRole role);
    static Result<NodeRole> ParseRole(const std::string& role);

    NodeInfo& EnsureNodeLocked(const std::string& node_id);
    std::string ResolveAddressLocked(const std::string& node_id) const;
    std::vector<std::string> SortedMentorIdsLocked() const;
    std::optional<std::string> FindFollowerIdOfLocked(const std::string& mentor_id) const;
    std::string SelectSourceForFollowerLocked(const std::string& follower_id) const;
    void ReassignFollowersLocked(const std::string& old_source, const std::string& exclude_node_id);
    void NormalizeMentorSourcesLocked();
    void EnsureLeaderLocked();

    mutable std::shared_mutex mutex_;
    std::string self_id_;
    std::string self_addr_;
    std::string leader_id_;
    std::unordered_map<std::string, std::string> registered_addrs_;
    std::unordered_map<std::string, NodeInfo> nodes_;
};
