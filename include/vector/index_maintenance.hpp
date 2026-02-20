#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>

#include "common/config.hpp"
#include "common/result.hpp"
#include "vector/vector_index.hpp"

enum class MaintenanceState { kIdle, kIsolating, kCompacting };

class IndexMaintenance {
public:
    explicit IndexMaintenance(IndexMaintenanceConfig config);
    ~IndexMaintenance();

    void Check(VectorIndex& index);

    MaintenanceState State() const noexcept;
    size_t CompactRuns() const noexcept;
    size_t IsolateRuns() const noexcept;
    Result<void> WaitForIdle(std::chrono::milliseconds timeout = std::chrono::seconds(5));

private:
    void RunCompact(VectorIndex& index);
    void RunIsolate(VectorIndex& index);
    void CleanupFinishedWorker(std::unique_lock<std::mutex>& lock);

    std::atomic<MaintenanceState> state_{MaintenanceState::kIdle};
    std::atomic<size_t> compact_runs_{0};
    std::atomic<size_t> isolate_runs_{0};
    std::chrono::steady_clock::time_point last_compact_time_;
    IndexMaintenanceConfig config_;

    mutable std::mutex mutex_;
    std::condition_variable idle_cv_;
    std::thread worker_;
};
