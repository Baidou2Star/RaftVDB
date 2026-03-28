#include "raft/topology.hpp"

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/logger.hpp"

namespace {

bool IsLeaderRole(NodeRole role) {
    return role == NodeRole::kLeader;
}

bool IsMentorRole(NodeRole role) {
    return role == NodeRole::kMentor;
}

bool IsFollowerRole(NodeRole role) {
    return role == NodeRole::kFollower;
}

} // namespace

TopologyManager::TopologyManager(std::string self_id, std::string self_addr)
    : self_id_(std::move(self_id)), self_addr_(std::move(self_addr)) {
    if (!self_id_.empty()) {
        RegisterNode(self_id_, self_addr_);
    }
}

void TopologyManager::SetSelf(std::string self_id, std::string self_addr) {
    std::unique_lock lock(mutex_);
    self_id_ = std::move(self_id);
    self_addr_ = std::move(self_addr);
    if (!self_id_.empty()) {
        registered_addrs_[self_id_] = self_addr_;
        NodeInfo& self = EnsureNodeLocked(self_id_);
        self.addr = ResolveAddressLocked(self_id_);
    }
}

void TopologyManager::SetLeader(const std::string& leader_id, const std::string& leader_addr) {
    std::unique_lock lock(mutex_);
    leader_id_ = leader_id;
    if (!leader_addr.empty()) {
        registered_addrs_[leader_id] = leader_addr;
        if (leader_id == self_id_) {
            self_addr_ = leader_addr;
        }
    }
    EnsureLeaderLocked();
}

void TopologyManager::RegisterNode(const std::string& node_id, const std::string& addr) {
    if (node_id.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);
    registered_addrs_[node_id] = addr;
    NodeInfo& node = EnsureNodeLocked(node_id);
    node.addr = ResolveAddressLocked(node_id);
}

void TopologyManager::RegisterNodes(const std::unordered_map<std::string, std::string>& nodes) {
    std::unique_lock lock(mutex_);
    for (const auto& [node_id, addr] : nodes) {
        if (node_id.empty()) {
            continue;
        }
        registered_addrs_[node_id] = addr;
        NodeInfo& node = EnsureNodeLocked(node_id);
        node.addr = ResolveAddressLocked(node_id);
    }
}

Result<void> TopologyManager::Rebalance(const std::vector<std::string>& healthy_nodes) {
    std::unique_lock lock(mutex_);
    if (leader_id_.empty()) {
        if (self_id_.empty()) {
            return Result<void>::Err("Rebalance 失败: 尚未设置 leader_id 或 self_id");
        }
        leader_id_ = self_id_;
    }

    EnsureLeaderLocked();

    // 先把当前拓扑清成“只有 Leader 活跃”，再根据新健康列表重建。
    for (auto& [node_id, node] : nodes_) {
        node.addr = ResolveAddressLocked(node_id);
        if (node_id == leader_id_) {
            node.role = NodeRole::kLeader;
            node.source_node_id.clear();
            node.healthy = true;
            continue;
        }
        node.role = NodeRole::kFollower;
        node.source_node_id.clear();
        node.healthy = false;
    }

    std::vector<std::string> candidates;
    candidates.reserve(healthy_nodes.size());
    std::unordered_set<std::string> seen;
    for (const auto& node_id : healthy_nodes) {
        if (node_id.empty() || node_id == leader_id_ || !seen.insert(node_id).second) {
            continue;
        }
        NodeInfo& node = EnsureNodeLocked(node_id);
        node.addr = ResolveAddressLocked(node_id);
        node.healthy = true;
        candidates.push_back(node_id);
    }

    // 定时拓扑刷新会被频繁触发。
    // 这里保持确定性的节点顺序，避免健康集合不变时也因为随机重排而产生无意义抖动。
    std::sort(candidates.begin(), candidates.end());

    const std::size_t mentor_count = (candidates.size() + 1U) / 2U;
    std::vector<std::string> mentors;
    mentors.reserve(mentor_count);

    for (std::size_t index = 0; index < candidates.size(); ++index) {
        NodeInfo& node = nodes_.at(candidates[index]);
        if (index < mentor_count) {
            node.role = NodeRole::kMentor;
            node.source_node_id = leader_id_;
            mentors.push_back(node.node_id);
        } else {
            node.role = NodeRole::kFollower;
            node.source_node_id = mentors[index - mentor_count];
        }
    }

    return Result<void>::Ok();
}

std::vector<NodeInfo> TopologyManager::GetMentors() const {
    std::shared_lock lock(mutex_);
    std::vector<NodeInfo> mentors;
    for (const auto& [_, node] : nodes_) {
        if (node.healthy && IsMentorRole(node.role)) {
            mentors.push_back(node);
        }
    }
    std::sort(mentors.begin(), mentors.end(),
              [](const NodeInfo& left, const NodeInfo& right) { return left.node_id < right.node_id; });
    return mentors;
}

std::optional<NodeInfo> TopologyManager::GetFollowerOf(const std::string& mentor_id) const {
    std::shared_lock lock(mutex_);
    auto follower_id = FindFollowerIdOfLocked(mentor_id);
    if (!follower_id) {
        return std::nullopt;
    }
    return nodes_.at(*follower_id);
}

std::optional<NodeInfo> TopologyManager::GetNode(const std::string& node_id) const {
    std::shared_lock lock(mutex_);
    auto found = nodes_.find(node_id);
    if (found == nodes_.end()) {
        return std::nullopt;
    }
    return found->second;
}

std::vector<NodeInfo> TopologyManager::AllNodes() const {
    std::shared_lock lock(mutex_);
    std::vector<NodeInfo> nodes;
    nodes.reserve(nodes_.size());
    for (const auto& [_, node] : nodes_) {
        nodes.push_back(node);
    }
    std::sort(nodes.begin(), nodes.end(),
              [](const NodeInfo& left, const NodeInfo& right) { return left.node_id < right.node_id; });
    return nodes;
}

NodeRole TopologyManager::GetRole(const std::string& node_id) const {
    std::shared_lock lock(mutex_);
    auto found = nodes_.find(node_id);
    if (found == nodes_.end()) {
        return NodeRole::kFollower;
    }
    return found->second.role;
}

std::string TopologyManager::GetSource(const std::string& node_id) const {
    std::shared_lock lock(mutex_);
    auto found = nodes_.find(node_id);
    if (found == nodes_.end()) {
        return {};
    }
    return found->second.source_node_id;
}

void TopologyManager::PromoteToMentor(const std::string& node_id) {
    if (node_id.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);
    EnsureLeaderLocked();
    NodeInfo& node = EnsureNodeLocked(node_id);
    node.healthy = true;
    node.role = (node_id == leader_id_) ? NodeRole::kLeader : NodeRole::kMentor;
    node.source_node_id = (node_id == leader_id_) ? std::string{} : leader_id_;
    node.addr = ResolveAddressLocked(node_id);
}

void TopologyManager::DemoteToFollower(const std::string& node_id) {
    if (node_id.empty() || node_id == leader_id_) {
        return;
    }

    std::unique_lock lock(mutex_);
    EnsureLeaderLocked();
    NodeInfo& node = EnsureNodeLocked(node_id);
    node.healthy = true;
    node.role = NodeRole::kFollower;
    node.source_node_id = SelectSourceForFollowerLocked(node_id);
    node.addr = ResolveAddressLocked(node_id);
    ReassignFollowersLocked(node_id, node_id);
}

void TopologyManager::SwapFollowers(const std::string& mentor_a, const std::string& mentor_b) {
    if (mentor_a.empty() || mentor_b.empty() || mentor_a == mentor_b) {
        return;
    }

    std::unique_lock lock(mutex_);
    auto follower_a = FindFollowerIdOfLocked(mentor_a);
    auto follower_b = FindFollowerIdOfLocked(mentor_b);

    if (follower_a) {
        nodes_.at(*follower_a).source_node_id = mentor_b;
    }
    if (follower_b) {
        nodes_.at(*follower_b).source_node_id = mentor_a;
    }
}

void TopologyManager::MarkUnhealthy(const std::string& node_id) {
    if (node_id.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);
    NodeInfo& node = EnsureNodeLocked(node_id);
    node.healthy = false;

    if (node_id == leader_id_) {
        node.role = NodeRole::kLeader;
        node.source_node_id.clear();
        return;
    }

    node.role = NodeRole::kFollower;
    node.source_node_id.clear();
    ReassignFollowersLocked(node_id, {});
}

void TopologyManager::MarkHealthy(const std::string& node_id) {
    if (node_id.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);
    EnsureLeaderLocked();
    NodeInfo& node = EnsureNodeLocked(node_id);
    node.healthy = true;
    node.addr = ResolveAddressLocked(node_id);
    if (node_id == leader_id_) {
        node.role = NodeRole::kLeader;
        node.source_node_id.clear();
        return;
    }
    if (node.source_node_id.empty()) {
        node.role = NodeRole::kFollower;
        node.source_node_id = SelectSourceForFollowerLocked(node_id);
    }
}

raftvdb::proto::TopologyInfo TopologyManager::ToProto() const {
    auto proto = ToProto(self_id_);
    if (!proto) {
        return {};
    }
    return *proto;
}

Result<raftvdb::proto::TopologyInfo> TopologyManager::ToProto(const std::string& node_id) const {
    std::shared_lock lock(mutex_);
    auto found = nodes_.find(node_id);
    if (found == nodes_.end()) {
        return Result<raftvdb::proto::TopologyInfo>::Err("ToProto 失败: 未找到节点 " + node_id);
    }

    raftvdb::proto::TopologyInfo proto;
    proto.set_role(RoleToString(found->second.role));
    proto.set_source_node_id(found->second.source_node_id);
    if (IsMentorRole(found->second.role)) {
        auto follower_id = FindFollowerIdOfLocked(node_id);
        if (follower_id) {
            proto.set_follower_node_id(*follower_id);
        }
    }
    return Result<raftvdb::proto::TopologyInfo>::Ok(std::move(proto));
}

Result<void> TopologyManager::FromProto(const raftvdb::proto::TopologyInfo& proto) {
    if (self_id_.empty()) {
        return Result<void>::Err("FromProto 失败: 尚未设置 self_id");
    }
    return FromProto(self_id_, proto, self_addr_);
}

Result<void> TopologyManager::FromProto(const std::string& node_id,
                                        const raftvdb::proto::TopologyInfo& proto,
                                        const std::string& node_addr) {
    if (node_id.empty()) {
        return Result<void>::Err("FromProto 失败: node_id 不能为空");
    }

    auto parsed_role = ParseRole(proto.role());
    if (!parsed_role) {
        return Result<void>::Err(parsed_role.error);
    }

    std::unique_lock lock(mutex_);
    std::optional<NodeRole> previous_role;
    std::string previous_source_node_id;
    std::string previous_follower_node_id;
    if (auto previous = nodes_.find(node_id); previous != nodes_.end()) {
        previous_role = previous->second.role;
        previous_source_node_id = previous->second.source_node_id;
        if (IsMentorRole(previous->second.role)) {
            if (auto follower = FindFollowerIdOfLocked(node_id)) {
                previous_follower_node_id = *follower;
            }
        }
    }

    if (!node_addr.empty()) {
        registered_addrs_[node_id] = node_addr;
        if (node_id == self_id_) {
            self_addr_ = node_addr;
        }
    }

    NodeInfo& node = EnsureNodeLocked(node_id);
    node.addr = ResolveAddressLocked(node_id);
    node.role = *parsed_role;
    node.source_node_id = proto.source_node_id();
    node.healthy = true;

    if (IsLeaderRole(node.role)) {
        leader_id_ = node_id;
        node.source_node_id.clear();
        EnsureLeaderLocked();
    } else if (!node.source_node_id.empty()) {
        NodeInfo& source = EnsureNodeLocked(node.source_node_id);
        source.addr = ResolveAddressLocked(source.node_id);
        source.healthy = true;
        if (IsMentorRole(node.role)) {
            leader_id_ = source.node_id;
            source.role = NodeRole::kLeader;
            source.source_node_id.clear();
        }
    }

    // Mentor 侧收到的 follower_node_id 是“我当前负责谁”的权威描述。
    // 因此即便该字段为空，也要先把本地缓存里旧的下游绑定清掉，
    // 避免 Mentor 长时间保留已失效的 follower_node_id。
    if (IsMentorRole(node.role)) {
        auto previous_follower = FindFollowerIdOfLocked(node_id);
        if (previous_follower.has_value() && *previous_follower != proto.follower_node_id()) {
            NodeInfo& stale_follower = nodes_.at(*previous_follower);
            stale_follower.role = NodeRole::kFollower;
            stale_follower.source_node_id.clear();
        }

        if (!proto.follower_node_id().empty()) {
            NodeInfo& follower = EnsureNodeLocked(proto.follower_node_id());
            follower.addr = ResolveAddressLocked(follower.node_id);
            follower.role = NodeRole::kFollower;
            follower.source_node_id = node_id;
            follower.healthy = true;
        }
    }

    NormalizeMentorSourcesLocked();

    std::string current_role = RoleToString(node.role);
    std::string current_source_node_id = node.source_node_id;
    std::string current_follower_node_id;
    if (IsMentorRole(node.role)) {
        if (auto follower = FindFollowerIdOfLocked(node_id)) {
            current_follower_node_id = *follower;
        }
    }
    const bool topology_changed =
        !previous_role.has_value() || *previous_role != node.role ||
        previous_source_node_id != current_source_node_id ||
        previous_follower_node_id != current_follower_node_id;
    lock.unlock();

    // 只在角色视图真正变化时打印一次拓扑日志，避免把心跳周期中的相同广播刷满文件。
    // 集成测试会依赖这条日志识别当前 Mentor / Follower 绑定关系。
    if (topology_changed) {
        LOG_INFO("TOPOLOGY_APPLIED", "node_id={}, role={}, source_node_id={}, follower_node_id={}",
                 node_id, current_role, current_source_node_id, current_follower_node_id);
    }
    return Result<void>::Ok();
}

std::optional<NodeInfo> TopologyManager::GetMentorOf(const std::string& follower_id) const {
    std::shared_lock lock(mutex_);
    auto found = nodes_.find(follower_id);
    if (found == nodes_.end() || !IsFollowerRole(found->second.role) ||
        found->second.source_node_id.empty()) {
        return std::nullopt;
    }

    auto mentor = nodes_.find(found->second.source_node_id);
    if (mentor == nodes_.end()) {
        return std::nullopt;
    }
    return mentor->second;
}

std::string TopologyManager::RoleToString(NodeRole role) {
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

Result<NodeRole> TopologyManager::ParseRole(const std::string& role) {
    if (role == "leader") {
        return Result<NodeRole>::Ok(NodeRole::kLeader);
    }
    if (role == "mentor") {
        return Result<NodeRole>::Ok(NodeRole::kMentor);
    }
    if (role == "follower") {
        return Result<NodeRole>::Ok(NodeRole::kFollower);
    }
    return Result<NodeRole>::Err("非法拓扑角色: " + role);
}

NodeInfo& TopologyManager::EnsureNodeLocked(const std::string& node_id) {
    auto [it, inserted] = nodes_.try_emplace(node_id);
    NodeInfo& node = it->second;
    if (inserted) {
        node.node_id = node_id;
        node.addr = ResolveAddressLocked(node_id);
        node.role = NodeRole::kFollower;
        node.source_node_id.clear();
        node.healthy = true;
    }
    if (node.node_id.empty()) {
        node.node_id = node_id;
    }
    if (node.addr.empty()) {
        node.addr = ResolveAddressLocked(node_id);
    }
    return node;
}

std::string TopologyManager::ResolveAddressLocked(const std::string& node_id) const {
    if (node_id == self_id_ && !self_addr_.empty()) {
        return self_addr_;
    }

    auto registered = registered_addrs_.find(node_id);
    if (registered != registered_addrs_.end() && !registered->second.empty()) {
        return registered->second;
    }

    auto existing = nodes_.find(node_id);
    if (existing != nodes_.end() && !existing->second.addr.empty()) {
        return existing->second.addr;
    }

    // 当前阶段配置里还没有完整的 node_id -> addr 强约束映射。
    // 若地址尚未注册，先回退为 node_id 本身，避免拓扑结构因为缺失地址而失效。
    return node_id;
}

std::vector<std::string> TopologyManager::SortedMentorIdsLocked() const {
    std::vector<std::string> mentors;
    for (const auto& [node_id, node] : nodes_) {
        if (node.healthy && IsMentorRole(node.role)) {
            mentors.push_back(node_id);
        }
    }
    std::sort(mentors.begin(), mentors.end());
    return mentors;
}

std::optional<std::string> TopologyManager::FindFollowerIdOfLocked(const std::string& mentor_id) const {
    for (const auto& [node_id, node] : nodes_) {
        if (node.healthy && IsFollowerRole(node.role) && node.source_node_id == mentor_id) {
            return node_id;
        }
    }
    return std::nullopt;
}

std::string TopologyManager::SelectSourceForFollowerLocked(const std::string& follower_id) const {
    for (const auto& mentor_id : SortedMentorIdsLocked()) {
        if (mentor_id == follower_id) {
            continue;
        }
        auto existing_follower = FindFollowerIdOfLocked(mentor_id);
        if (!existing_follower || *existing_follower == follower_id) {
            return mentor_id;
        }
    }
    if (!leader_id_.empty() && leader_id_ != follower_id) {
        return leader_id_;
    }
    return {};
}

void TopologyManager::ReassignFollowersLocked(const std::string& old_source,
                                              const std::string& exclude_node_id) {
    if (old_source.empty()) {
        return;
    }

    std::vector<std::string> to_reassign;
    for (const auto& [node_id, node] : nodes_) {
        if (!node.healthy || !IsFollowerRole(node.role)) {
            continue;
        }
        if (node.source_node_id != old_source || node_id == exclude_node_id) {
            continue;
        }
        to_reassign.push_back(node_id);
    }

    std::sort(to_reassign.begin(), to_reassign.end());
    for (const auto& follower_id : to_reassign) {
        nodes_.at(follower_id).source_node_id = SelectSourceForFollowerLocked(follower_id);
    }
}

void TopologyManager::NormalizeMentorSourcesLocked() {
    if (leader_id_.empty()) {
        return;
    }

    EnsureLeaderLocked();
    for (auto& [node_id, node] : nodes_) {
        if (node_id == leader_id_) {
            node.role = NodeRole::kLeader;
            node.source_node_id.clear();
            continue;
        }
        if (IsMentorRole(node.role)) {
            node.source_node_id = leader_id_;
        }
    }
}

void TopologyManager::EnsureLeaderLocked() {
    if (leader_id_.empty()) {
        return;
    }

    NodeInfo& leader = EnsureNodeLocked(leader_id_);
    leader.role = NodeRole::kLeader;
    leader.source_node_id.clear();
    leader.healthy = true;
    leader.addr = ResolveAddressLocked(leader_id_);
}
