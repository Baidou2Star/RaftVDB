#include "common/config.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <initializer_list>
#include <optional>
#include <sstream>
#include <string_view>

#include <toml++/toml.hpp>

namespace {

template <typename T>
Result<std::optional<T>> TryReadValue(const toml::table& table, std::string_view path) {
    auto node = table.at_path(path);
    if (!node) {
        return Result<std::optional<T>>::Ok(std::nullopt);
    }

    auto value = node.template value<T>();
    if (!value) {
        return Result<std::optional<T>>::Err("配置项类型错误: " + std::string(path));
    }

    return Result<std::optional<T>>::Ok(std::optional<T>{*value});
}

template <typename T>
Result<T> ReadRequiredValue(const toml::table& table, std::string_view path) {
    auto value = TryReadValue<T>(table, path);
    if (!value) {
        return Result<T>::Err(value.error);
    }
    if (!value->has_value()) {
        return Result<T>::Err("缺少必填配置项: " + std::string(path));
    }
    return Result<T>::Ok(std::move(**value));
}

template <typename T>
Result<T> ReadRequiredValueWithAliases(const toml::table& table,
                                       std::initializer_list<std::string_view> paths) {
    std::ostringstream expected_keys;
    bool first = true;
    for (auto path : paths) {
        auto value = TryReadValue<T>(table, path);
        if (!value) {
            return Result<T>::Err(value.error);
        }
        if (value->has_value()) {
            return Result<T>::Ok(std::move(**value));
        }

        if (!first) {
            expected_keys << ", ";
        }
        expected_keys << path;
        first = false;
    }

    return Result<T>::Err("缺少必填配置项，候选键为: " + expected_keys.str());
}

template <typename T>
Result<T> ReadOptionalValue(const toml::table& table, std::string_view path, T default_value) {
    auto value = TryReadValue<T>(table, path);
    if (!value) {
        return Result<T>::Err(value.error);
    }
    if (!value->has_value()) {
        return Result<T>::Ok(std::move(default_value));
    }
    return Result<T>::Ok(std::move(**value));
}

template <typename T>
Result<T> ReadOptionalValueWithAliases(const toml::table& table,
                                       std::initializer_list<std::string_view> paths,
                                       T default_value) {
    for (auto path : paths) {
        auto value = TryReadValue<T>(table, path);
        if (!value) {
            return Result<T>::Err(value.error);
        }
        if (value->has_value()) {
            return Result<T>::Ok(std::move(**value));
        }
    }
    return Result<T>::Ok(std::move(default_value));
}

Result<std::vector<std::string>> ReadStringArray(const toml::table& table, std::string_view path) {
    auto node = table.at_path(path);
    if (!node) {
        return Result<std::vector<std::string>>::Ok({});
    }
    if (!node.is_array()) {
        return Result<std::vector<std::string>>::Err("配置项类型错误: " + std::string(path));
    }

    std::vector<std::string> values;
    for (const auto& item : *node.as_array()) {
        auto value = item.value<std::string>();
        if (!value) {
            return Result<std::vector<std::string>>::Err("配置数组元素类型错误: " + std::string(path));
        }
        values.push_back(*value);
    }
    return Result<std::vector<std::string>>::Ok(std::move(values));
}

std::string ToLower(std::string value) {
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

template <typename... Results>
Result<void> EnsureAllOk(const Results&... results) {
    Result<void> status = Result<void>::Ok();
    const auto check_one = [&status](const auto& result) {
        if (static_cast<bool>(status) && !static_cast<bool>(result)) {
            status = Result<void>::Err(result.error);
        }
    };
    (check_one(results), ...);
    return status;
}

Result<void> ValidateVectorConfig(const VectorConfig& cfg) {
    if (cfg.dim == 0) {
        return Result<void>::Err("vector.dim 必须大于 0");
    }
    if (cfg.initial_capacity == 0) {
        return Result<void>::Err("vector.initial_capacity 必须大于 0");
    }
    if (cfg.connectivity == 0) {
        return Result<void>::Err("vector.connectivity 必须大于 0");
    }
    if (cfg.expansion_add == 0 || cfg.expansion_search == 0) {
        return Result<void>::Err("vector 扩展参数必须大于 0");
    }
    if (cfg.metric != "ip" && cfg.metric != "l2sq" && cfg.metric != "cos") {
        return Result<void>::Err("vector.metric 仅支持 ip、l2sq、cos");
    }
    if (cfg.data_type != "f32" && cfg.data_type != "f16" && cfg.data_type != "i8") {
        return Result<void>::Err("vector.data_type 仅支持 f32、f16、i8");
    }
    return Result<void>::Ok();
}

Result<void> ValidateIndexMaintenanceConfig(const IndexMaintenanceConfig& cfg) {
    if (cfg.check_interval_s == 0) {
        return Result<void>::Err("index_maintenance.check_interval_s 必须大于 0");
    }
    if (cfg.compact_max_interval_s == 0) {
        return Result<void>::Err("index_maintenance.compact_max_interval_s 必须大于 0");
    }
    if (cfg.compact_delete_ratio_threshold < 0.0f || cfg.compact_delete_ratio_threshold > 1.0f) {
        return Result<void>::Err("compact_delete_ratio_threshold 必须位于 [0, 1]");
    }
    if (cfg.isolate_fragmentation_threshold < 0.0f ||
        cfg.isolate_fragmentation_threshold > 1.0f) {
        return Result<void>::Err("isolate_fragmentation_threshold 必须位于 [0, 1]");
    }
    return Result<void>::Ok();
}

Result<void> ValidateClientConfig(const ClientConfig& cfg) {
    if (cfg.dedup_window_size == 0) {
        return Result<void>::Err("client.dedup_window_size 必须大于 0");
    }
    if (cfg.retry_base_ms == 0 || cfg.retry_max_ms == 0) {
        return Result<void>::Err("client 重试时间必须大于 0");
    }
    if (cfg.retry_base_ms > cfg.retry_max_ms) {
        return Result<void>::Err("client.retry_base_ms 不能大于 client.retry_max_ms");
    }
    if (cfg.max_retry_count == 0) {
        return Result<void>::Err("client.max_retry_count 必须大于 0");
    }
    return Result<void>::Ok();
}

Result<void> ValidateServerConfig(const ServerConfig& cfg) {
    if (cfg.grpc_port == 0) {
        return Result<void>::Err("server.grpc_port 必须大于 0");
    }
    return Result<void>::Ok();
}

Result<void> ValidateStorageConfig(const StorageConfig& cfg) {
    if (cfg.raft_log_dir.empty()) {
        return Result<void>::Err("storage.raft_log_dir 不能为空");
    }
    if (cfg.snapshot_dir.empty()) {
        return Result<void>::Err("storage.snapshot_dir 不能为空");
    }
    return Result<void>::Ok();
}

} // namespace

Result<void> ValidateLeaseConfig(const RaftConfig& cfg) {
    if (cfg.heartbeat_interval_ms == 0) {
        return Result<void>::Err("raft.heartbeat_interval_ms 必须大于 0");
    }
    if (cfg.election_timeout_min_ms == 0) {
        return Result<void>::Err("raft.election_timeout_min_ms 必须大于 0");
    }
    if (cfg.election_timeout_max_ms < cfg.election_timeout_min_ms) {
        return Result<void>::Err("raft.election_timeout_max_ms 不能小于 raft.election_timeout_min_ms");
    }
    if (cfg.heartbeat_interval_ms >= cfg.election_timeout_min_ms) {
        return Result<void>::Err("raft.heartbeat_interval_ms 必须小于 raft.election_timeout_min_ms");
    }
    if (cfg.clock_drift_bound_ms >= cfg.election_timeout_min_ms) {
        return Result<void>::Err("raft.clock_drift_bound_ms 必须小于 raft.election_timeout_min_ms");
    }

    const auto expected_lease_duration =
        cfg.election_timeout_min_ms - cfg.clock_drift_bound_ms;
    if (cfg.lease_duration_ms != expected_lease_duration) {
        return Result<void>::Err("raft.lease_duration_ms 必须等于 election_timeout_min_ms - "
                                 "clock_drift_bound_ms");
    }

    return Result<void>::Ok();
}

Result<Config> Config::LoadFromFile(const std::string& path) {
    try {
        toml::table table = toml::parse_file(path);

        Config cfg;

        auto node_id = ReadRequiredValue<std::string>(table, "cluster.node_id");
        if (!node_id) {
            return Result<Config>::Err(node_id.error);
        }
        auto peers = ReadStringArray(table, "cluster.peers");
        if (!peers) {
            return Result<Config>::Err(peers.error);
        }
        cfg.cluster.node_id = std::move(*node_id);
        cfg.cluster.peers = std::move(*peers);
        if (cfg.cluster.node_id.empty()) {
            return Result<Config>::Err("cluster.node_id 不能为空");
        }

        auto heartbeat = ReadOptionalValue<uint32_t>(
            table, "raft.heartbeat_interval_ms", cfg.raft.heartbeat_interval_ms);
        auto election_min = ReadOptionalValue<uint32_t>(
            table, "raft.election_timeout_min_ms", cfg.raft.election_timeout_min_ms);
        auto election_max = ReadOptionalValue<uint32_t>(
            table, "raft.election_timeout_max_ms", cfg.raft.election_timeout_max_ms);
        auto clock_drift = ReadOptionalValue<uint32_t>(
            table, "raft.clock_drift_bound_ms", cfg.raft.clock_drift_bound_ms);
        auto explicit_lease = TryReadValue<uint32_t>(table, "raft.lease_duration_ms");
        auto snapshot_threshold = ReadOptionalValue<uint32_t>(
            table, "raft.snapshot_threshold", cfg.raft.snapshot_threshold);
        auto pipeline_window = ReadOptionalValue<uint32_t>(
            table, "raft.pipeline_window_size", cfg.raft.pipeline_window_size);
        auto batch_max = ReadOptionalValue<uint32_t>(
            table, "raft.batch_max_entries", cfg.raft.batch_max_entries);
        auto batch_flush = ReadOptionalValue<uint32_t>(
            table, "raft.batch_flush_interval_us", cfg.raft.batch_flush_interval_us);
        auto mentor_ack = ReadOptionalValue<uint32_t>(
            table, "raft.mentor_ack_timeout_ms", cfg.raft.mentor_ack_timeout_ms);
        auto mentor_recover = ReadOptionalValue<uint32_t>(
            table, "raft.mentor_recover_timeout_ms", cfg.raft.mentor_recover_timeout_ms);
        auto topology_refresh = ReadOptionalValue<uint32_t>(
            table, "raft.topology_refresh_interval_ms", cfg.raft.topology_refresh_interval_ms);

        auto raft_status = EnsureAllOk(heartbeat,
                                       election_min,
                                       election_max,
                                       clock_drift,
                                       explicit_lease,
                                       snapshot_threshold,
                                       pipeline_window,
                                       batch_max,
                                       batch_flush,
                                       mentor_ack,
                                       mentor_recover,
                                       topology_refresh);
        if (!raft_status) {
            return Result<Config>::Err(raft_status.error);
        }

        cfg.raft.heartbeat_interval_ms = *heartbeat;
        cfg.raft.election_timeout_min_ms = *election_min;
        cfg.raft.election_timeout_max_ms = *election_max;
        cfg.raft.clock_drift_bound_ms = *clock_drift;
        cfg.raft.snapshot_threshold = *snapshot_threshold;
        cfg.raft.pipeline_window_size = *pipeline_window;
        cfg.raft.batch_max_entries = *batch_max;
        cfg.raft.batch_flush_interval_us = *batch_flush;
        cfg.raft.mentor_ack_timeout_ms = *mentor_ack;
        cfg.raft.mentor_recover_timeout_ms = *mentor_recover;
        cfg.raft.topology_refresh_interval_ms = *topology_refresh;
        cfg.raft.lease_duration_ms = explicit_lease->value_or(
            cfg.raft.election_timeout_min_ms - cfg.raft.clock_drift_bound_ms);

        auto dim = ReadRequiredValueWithAliases<uint32_t>(
            table, {"vector.dim", "vector.dimension"});
        auto metric = ReadOptionalValue<std::string>(table, "vector.metric", cfg.vector.metric);
        auto data_type = ReadRequiredValueWithAliases<std::string>(
            table, {"vector.data_type", "vector.scalar_kind"});
        auto initial_capacity = ReadOptionalValue<uint32_t>(
            table, "vector.initial_capacity", cfg.vector.initial_capacity);
        auto connectivity = ReadOptionalValue<uint32_t>(
            table, "vector.connectivity", cfg.vector.connectivity);
        auto expansion_add = ReadOptionalValue<uint32_t>(
            table, "vector.expansion_add", cfg.vector.expansion_add);
        auto expansion_search = ReadOptionalValue<uint32_t>(
            table, "vector.expansion_search", cfg.vector.expansion_search);

        auto vector_status = EnsureAllOk(
            dim, metric, data_type, initial_capacity, connectivity, expansion_add, expansion_search);
        if (!vector_status) {
            return Result<Config>::Err(vector_status.error);
        }

        cfg.vector.dim = *dim;
        cfg.vector.metric = ToLower(*metric);
        cfg.vector.data_type = ToLower(*data_type);
        cfg.vector.initial_capacity = *initial_capacity;
        cfg.vector.connectivity = *connectivity;
        cfg.vector.expansion_add = *expansion_add;
        cfg.vector.expansion_search = *expansion_search;

        auto maintenance_interval = ReadOptionalValue<uint32_t>(
            table,
            "index_maintenance.check_interval_s",
            cfg.index_maintenance.check_interval_s);
        auto compact_ratio = ReadOptionalValue<float>(
            table,
            "index_maintenance.compact_delete_ratio_threshold",
            cfg.index_maintenance.compact_delete_ratio_threshold);
        auto compact_interval = ReadOptionalValue<uint32_t>(
            table,
            "index_maintenance.compact_max_interval_s",
            cfg.index_maintenance.compact_max_interval_s);
        auto isolate_ratio = ReadOptionalValue<float>(
            table,
            "index_maintenance.isolate_fragmentation_threshold",
            cfg.index_maintenance.isolate_fragmentation_threshold);

        auto maintenance_status =
            EnsureAllOk(maintenance_interval, compact_ratio, compact_interval, isolate_ratio);
        if (!maintenance_status) {
            return Result<Config>::Err(maintenance_status.error);
        }

        cfg.index_maintenance.check_interval_s = *maintenance_interval;
        cfg.index_maintenance.compact_delete_ratio_threshold = *compact_ratio;
        cfg.index_maintenance.compact_max_interval_s = *compact_interval;
        cfg.index_maintenance.isolate_fragmentation_threshold = *isolate_ratio;

        auto dedup_window = ReadOptionalValue<uint32_t>(
            table, "client.dedup_window_size", cfg.client.dedup_window_size);
        auto retry_base = ReadOptionalValue<uint32_t>(
            table, "client.retry_base_ms", cfg.client.retry_base_ms);
        auto retry_max = ReadOptionalValue<uint32_t>(
            table, "client.retry_max_ms", cfg.client.retry_max_ms);
        auto retry_count = ReadOptionalValue<uint32_t>(
            table, "client.max_retry_count", cfg.client.max_retry_count);

        auto client_status = EnsureAllOk(dedup_window, retry_base, retry_max, retry_count);
        if (!client_status) {
            return Result<Config>::Err(client_status.error);
        }

        cfg.client.dedup_window_size = *dedup_window;
        cfg.client.retry_base_ms = *retry_base;
        cfg.client.retry_max_ms = *retry_max;
        cfg.client.max_retry_count = *retry_count;

        auto grpc_port =
            ReadOptionalValue<uint16_t>(table, "server.grpc_port", cfg.server.grpc_port);
        if (!grpc_port) {
            return Result<Config>::Err(grpc_port.error);
        }
        cfg.server.grpc_port = *grpc_port;

        auto raft_log_dir = TryReadValue<std::string>(table, "storage.raft_log_dir");
        auto snapshot_dir = TryReadValue<std::string>(table, "storage.snapshot_dir");
        auto data_dir = TryReadValue<std::string>(table, "storage.data_dir");

        if (!raft_log_dir) {
            return Result<Config>::Err(raft_log_dir.error);
        }
        if (!snapshot_dir) {
            return Result<Config>::Err(snapshot_dir.error);
        }
        if (!data_dir) {
            return Result<Config>::Err(data_dir.error);
        }

        if (raft_log_dir->has_value()) {
            cfg.storage.raft_log_dir = std::move(**raft_log_dir);
        }
        if (snapshot_dir->has_value()) {
            cfg.storage.snapshot_dir = std::move(**snapshot_dir);
        }
        if ((!raft_log_dir->has_value() || !snapshot_dir->has_value()) && data_dir->has_value()) {
            const std::filesystem::path base_dir(**data_dir);
            if (!raft_log_dir->has_value()) {
                cfg.storage.raft_log_dir = (base_dir / "raft").string();
            }
            if (!snapshot_dir->has_value()) {
                cfg.storage.snapshot_dir = (base_dir / "snapshot").string();
            }
        }

        auto lease_validation = ValidateLeaseConfig(cfg.raft);
        if (!lease_validation) {
            return Result<Config>::Err(lease_validation.error);
        }

        auto vector_validation = ValidateVectorConfig(cfg.vector);
        if (!vector_validation) {
            return Result<Config>::Err(vector_validation.error);
        }

        auto maintenance_validation = ValidateIndexMaintenanceConfig(cfg.index_maintenance);
        if (!maintenance_validation) {
            return Result<Config>::Err(maintenance_validation.error);
        }

        auto client_validation = ValidateClientConfig(cfg.client);
        if (!client_validation) {
            return Result<Config>::Err(client_validation.error);
        }

        auto server_validation = ValidateServerConfig(cfg.server);
        if (!server_validation) {
            return Result<Config>::Err(server_validation.error);
        }

        auto storage_validation = ValidateStorageConfig(cfg.storage);
        if (!storage_validation) {
            return Result<Config>::Err(storage_validation.error);
        }

        return Result<Config>::Ok(std::move(cfg));
    } catch (const toml::parse_error& error) {
        return Result<Config>::Err("解析配置文件失败: " + std::string(error.description()));
    } catch (const std::exception& error) {
        return Result<Config>::Err("加载配置文件失败: " + std::string(error.what()));
    }
}
