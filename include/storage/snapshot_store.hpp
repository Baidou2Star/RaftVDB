#pragma once

#include <cstdint>
#include <string>

#include "common/config.hpp"
#include "common/result.hpp"

struct SnapshotMeta {
    uint64_t raft_term = 0;
    uint64_t raft_index = 0;
    uint32_t dim = 0;
    std::string metric;
    std::string data_type;
    std::string created_at;

    Result<void> SaveToFile(const std::string& path) const;
    static Result<SnapshotMeta> LoadFromFile(const std::string& path);
    Result<void> ValidateAgainstConfig(const VectorConfig& config) const;
};

class SnapshotStore {
public:
    explicit SnapshotStore(std::string snapshot_dir);

    Result<void> Initialize() const;
    Result<void> FinalizeSnapshot(const SnapshotMeta& meta) const;
    bool HasSnapshot() const;
    Result<SnapshotMeta> LoadLatest(const VectorConfig& config) const;

    std::string SnapshotPath() const;
    std::string MetaPath() const;
    std::string TemporarySnapshotPath() const;

private:
    Result<void> CleanupTemporaryFiles() const;
    std::string TemporaryMetaPath() const;

    std::string snapshot_dir_;
};
