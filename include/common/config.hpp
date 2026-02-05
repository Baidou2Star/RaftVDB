#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/result.hpp"

struct ClusterConfig {
    std::string node_id;
    std::vector<std::string> peers;
};

struct RaftConfig {
    uint32_t heartbeat_interval_ms = 150;
    uint32_t election_timeout_min_ms = 300;
    uint32_t election_timeout_max_ms = 600;
    uint32_t clock_drift_bound_ms = 50;
    uint32_t lease_duration_ms = 250;
    uint32_t snapshot_threshold = 10000;
    uint32_t pipeline_window_size = 16;
    uint32_t batch_max_entries = 64;
    uint32_t batch_flush_interval_us = 500;
    uint32_t mentor_ack_timeout_ms = 200;
    uint32_t mentor_recover_timeout_ms = 3000;
    uint32_t topology_refresh_interval_ms = 30000;
};

struct VectorConfig {
    uint32_t dim = 1024;
    std::string metric = "ip";
    std::string data_type = "f32";
    uint32_t initial_capacity = 100000;
    uint32_t connectivity = 16;
    uint32_t expansion_add = 128;
    uint32_t expansion_search = 64;
};

struct IndexMaintenanceConfig {
    uint32_t check_interval_s = 60;
    float compact_delete_ratio_threshold = 0.2f;
    uint32_t compact_max_interval_s = 604800;
    float isolate_fragmentation_threshold = 0.1f;
};

struct ClientConfig {
    uint32_t dedup_window_size = 1000;
    uint32_t retry_base_ms = 50;
    uint32_t retry_max_ms = 2000;
    uint32_t max_retry_count = 10;
};

struct ServerConfig {
    uint16_t grpc_port = 7001;
};

struct StorageConfig {
    std::string raft_log_dir = "./data/raft";
    std::string snapshot_dir = "./data/snapshot";
};

struct Config {
    ClusterConfig cluster;
    RaftConfig raft;
    VectorConfig vector;
    IndexMaintenanceConfig index_maintenance;
    ClientConfig client;
    ServerConfig server;
    StorageConfig storage;

    static Result<Config> LoadFromFile(const std::string& path);

    bool IsSingleNode() const noexcept {
        // We treat both an empty peer list and a single configured endpoint as
        // single-node bootstrap modes so the loader stays compatible with the
        // current docs and the earlier bootstrap template.
        return cluster.peers.size() <= 1;
    }
};

Result<void> ValidateLeaseConfig(const RaftConfig& cfg);
