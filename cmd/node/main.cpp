#include <csignal>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "client/dedup.hpp"
#include "common/defer.hpp"
#include "common/logger.hpp"
#include "common/perf_counters.hpp"
#include "raft/raft_node.hpp"
#include "raft/raft_server.hpp"
#include "storage/snapshot_store.hpp"
#include "vector/vector_index.hpp"

namespace {

std::atomic<int> g_signal_count{0};

void HandleTerminationSignal(int) {
    g_signal_count.fetch_add(1, std::memory_order_acq_rel);
}

bool LooksLikeAddress(const std::string& value) {
    return value.find(':') != std::string::npos;
}

std::string DedupSnapshotPath(const Config& config) {
    return (std::filesystem::path(config.storage.snapshot_dir) / "dedup.bin").string();
}

Result<Config> LoadAndNormalizeConfig(const std::string& config_path) {
    auto config = Config::LoadFromFile(config_path);
    if (!config) {
        return Result<Config>::Err(config.error);
    }

    // 当前实现里 cluster.peers 仍同时承担“节点标识 + gRPC 地址”的语义，
    // 因此多节点模式下要求 cluster.node_id 也能直接作为地址使用。
    if (!LooksLikeAddress(config->cluster.node_id)) {
        if (!config->IsSingleNode()) {
            return Result<Config>::Err(
                "当前实现要求多节点模式下 cluster.node_id 使用 host:port 形式的本机 gRPC 地址");
        }

        config->cluster.node_id = "127.0.0.1:" + std::to_string(config->server.grpc_port);
    }

    std::vector<std::string> normalized_peers;
    normalized_peers.reserve(config->cluster.peers.size());
    for (const auto& peer : config->cluster.peers) {
        if (peer.empty() || peer == config->cluster.node_id) {
            continue;
        }
        normalized_peers.push_back(peer);
    }
    config->cluster.peers = std::move(normalized_peers);
    return config;
}

Result<std::shared_ptr<VectorIndex>> RestoreVectorIndex(const Config& config,
                                                        const SnapshotStore& snapshot_store,
                                                        bool* restored_from_snapshot,
                                                        SnapshotMeta* restored_snapshot_meta) {
    *restored_from_snapshot = false;
    *restored_snapshot_meta = {};

    if (!snapshot_store.HasSnapshot()) {
        auto created = VectorIndex::Create(config.vector);
        if (!created) {
            return Result<std::shared_ptr<VectorIndex>>::Err(created.error);
        }
        return created;
    }

    auto meta = snapshot_store.LoadLatest(config.vector);
    if (!meta) {
        return Result<std::shared_ptr<VectorIndex>>::Err(meta.error);
    }

    auto loaded = VectorIndex::LoadFromSnapshot(snapshot_store.SnapshotPath(), config.vector);
    if (!loaded) {
        return Result<std::shared_ptr<VectorIndex>>::Err(loaded.error);
    }

    *restored_from_snapshot = true;
    *restored_snapshot_meta = *meta;
    return loaded;
}

Result<std::shared_ptr<DedupTable>> RestoreDedupTable(const Config& config,
                                                      bool restored_from_snapshot) {
    auto dedup_table = std::make_shared<DedupTable>(config.client.dedup_window_size);
    if (!restored_from_snapshot) {
        return Result<std::shared_ptr<DedupTable>>::Ok(std::move(dedup_table));
    }

    const auto dedup_path = DedupSnapshotPath(config);
    if (!std::filesystem::exists(dedup_path)) {
        LOG_WARN("DEDUP_SNAPSHOT_MISSING", "snapshot_dir={}, path={}", config.storage.snapshot_dir,
                 dedup_path);
        return Result<std::shared_ptr<DedupTable>>::Ok(std::move(dedup_table));
    }

    auto loaded = dedup_table->LoadFrom(dedup_path);
    if (!loaded) {
        return Result<std::shared_ptr<DedupTable>>::Err(loaded.error);
    }
    return Result<std::shared_ptr<DedupTable>>::Ok(std::move(dedup_table));
}

Result<void> InstallSignalHandlers() {
    struct sigaction action {};
    action.sa_handler = HandleTerminationSignal;
    sigemptyset(&action.sa_mask);

    if (sigaction(SIGINT, &action, nullptr) != 0) {
        return Result<void>::Err("安装 SIGINT 处理器失败");
    }
    if (sigaction(SIGTERM, &action, nullptr) != 0) {
        return Result<void>::Err("安装 SIGTERM 处理器失败");
    }
    return Result<void>::Ok();
}

void WaitForShutdownSignal() {
    while (g_signal_count.load(std::memory_order_acquire) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void DrainCommittedEntries(const std::shared_ptr<RaftNode>& node) {
    // 第一个信号触发“优雅退出”，第二个信号允许跳过等待，避免异常场景下无限阻塞。
    while (node->AppliedIndex() < node->CommitIndex()) {
        if (g_signal_count.load(std::memory_order_acquire) >= 2) {
            LOG_WARN("NODE_FORCE_EXIT", "commit_index={}, applied_index={}", node->CommitIndex(),
                     node->AppliedIndex());
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

} // namespace

int main(int argc, char** argv) {
    const std::string config_path = argc > 1 ? argv[1] : "config/node.toml";

    auto config = LoadAndNormalizeConfig(config_path);
    if (!config) {
        std::cerr << "加载配置失败: " << config.error << '\n';
        return 1;
    }

    auto logger = InitializeLogger(config->logging.level);
    if (!logger) {
        std::cerr << "初始化日志失败: " << logger.error << '\n';
        return 1;
    }
    Defer shutdown_logger([]() {
        FlushLogger();
        ShutdownLogger();
    });

    LOG_INFO("NODE_BOOTSTRAP_BEGIN", "config_path={}, node_id={}, peer_count={}", config_path,
             config->cluster.node_id, config->cluster.peers.size());

    SnapshotStore snapshot_store(config->storage.snapshot_dir);
    auto init_snapshot_store = snapshot_store.Initialize();
    if (!init_snapshot_store) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=init_snapshot_store, error={}",
                  init_snapshot_store.error);
        return 1;
    }

    bool restored_from_snapshot = false;
    SnapshotMeta restored_snapshot_meta;
    auto vector_index =
        RestoreVectorIndex(*config, snapshot_store, &restored_from_snapshot, &restored_snapshot_meta);
    if (!vector_index) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=restore_vector_index, error={}", vector_index.error);
        return 1;
    }

    auto dedup_table = RestoreDedupTable(*config, restored_from_snapshot);
    if (!dedup_table) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=restore_dedup, error={}", dedup_table.error);
        return 1;
    }

    if (restored_from_snapshot) {
        LOG_INFO("NODE_SNAPSHOT_RESTORED", "snapshot_dir={}", config->storage.snapshot_dir);
    } else {
        LOG_INFO("NODE_SNAPSHOT_EMPTY", "snapshot_dir={}", config->storage.snapshot_dir);
    }

    RaftNodeOptions options;
    options.self_addr = config->cluster.node_id;
    options.vector_index = *vector_index;
    options.dedup_table = *dedup_table;
    options.restored_snapshot_index = restored_snapshot_meta.raft_index;
    options.restored_snapshot_term = restored_snapshot_meta.raft_term;

    auto node = RaftNode::Create(*config, options);
    if (!node) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=create_node, error={}", node.error);
        return 1;
    }

    RaftServer server(*node, RaftServerOptions{.listen_addr = config->cluster.node_id});
    auto server_started = server.Start();
    if (!server_started) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=start_server, error={}", server_started.error);
        return 1;
    }
    Defer stop_server([&server]() { server.Stop(); });

    auto node_started = (*node)->Start();
    if (!node_started) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=start_node, error={}", node_started.error);
        return 1;
    }
    Defer stop_node([&node]() { (*node)->Stop(); });

    auto installed_signals = InstallSignalHandlers();
    if (!installed_signals) {
        LOG_ERROR("NODE_BOOTSTRAP_FAILED", "step=install_signal_handlers, error={}",
                  installed_signals.error);
        return 1;
    }

    LOG_INFO("NODE_BOOTSTRAP_READY", "listen_addr={}, single_node={}", server.BoundAddress(),
             config->IsSingleNode());

    WaitForShutdownSignal();

    LOG_INFO("NODE_SHUTDOWN_BEGIN", "listen_addr={}, commit_index={}, applied_index={}",
             server.BoundAddress(), (*node)->CommitIndex(), (*node)->AppliedIndex());

    // 先关闭 gRPC 入口，阻止新的客户端与复制请求进入；
    // 然后等待 ApplyLoop 把当前已提交日志全部落到状态机。
    server.Stop();
    DrainCommittedEntries(*node);
    (*node)->Stop();

    LOG_INFO("NODE_SHUTDOWN_DONE", "commit_index={}, applied_index={}", (*node)->CommitIndex(),
             (*node)->AppliedIndex());

    // 输出热路径延迟统计到 perf_stats.csv（与配置文件同目录）
    const auto stats_path =
        (std::filesystem::path(config_path).parent_path() / "perf_stats.csv").string();
    g_perf.DumpToFile(stats_path);

    return 0;
}
