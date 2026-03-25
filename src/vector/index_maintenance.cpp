#include "vector/index_maintenance.hpp"

#include <string>
#include <utility>

#include "common/logger.hpp"

namespace {

double SafeRatio(size_t numerator, size_t denominator) {
    if (denominator == 0U) {
        return 0.0;
    }
    return static_cast<double>(numerator) / static_cast<double>(denominator);
}

std::string MaintenanceStateName(MaintenanceState state) {
    switch (state) {
        case MaintenanceState::kIdle:
            return "idle";
        case MaintenanceState::kIsolating:
            return "isolating";
        case MaintenanceState::kCompacting:
            return "compacting";
    }
    return "idle";
}

} // namespace

IndexMaintenance::IndexMaintenance(IndexMaintenanceConfig config)
    : last_compact_time_(std::chrono::steady_clock::now()), config_(std::move(config)) {}

IndexMaintenance::~IndexMaintenance() {
    auto wait = WaitForIdle(std::chrono::seconds(30));
    (void)wait;
}

void IndexMaintenance::Check(VectorIndex& index) {
    const size_t total_slots = index.TotalSlots();
    const size_t size = index.Size();
    const size_t deleted = index.DeletedCount();

    const double delete_ratio = SafeRatio(deleted, total_slots);
    // 当前 VectorIndex 暴露的可观测统计里，“碎片率”只能先用空洞槽位占比做代理值。
    const double fragmentation_ratio = SafeRatio(total_slots - size, total_slots);
    const auto now = std::chrono::steady_clock::now();

    std::unique_lock lock(mutex_);
    CleanupFinishedWorker(lock);
    if (state_.load() != MaintenanceState::kIdle) {
        return;
    }

    const bool compact_overdue =
        config_.compact_max_interval_s == 0U ||
        now - last_compact_time_ >= std::chrono::seconds(config_.compact_max_interval_s);

    MaintenanceState next_state = MaintenanceState::kIdle;
    if (delete_ratio > static_cast<double>(config_.compact_delete_ratio_threshold) || compact_overdue) {
        next_state = MaintenanceState::kCompacting;
    } else if (fragmentation_ratio >
               static_cast<double>(config_.isolate_fragmentation_threshold)) {
        next_state = MaintenanceState::kIsolating;
    } else {
        return;
    }

    LOG_DEBUG("INDEX_MAINTENANCE_TRIGGER",
              "state={}, size={}, total_slots={}, deleted={}, delete_ratio={:.3f}, "
              "fragmentation_ratio={:.3f}",
              MaintenanceStateName(next_state),
              size,
              total_slots,
              deleted,
              delete_ratio,
              fragmentation_ratio);

    state_.store(next_state);
    try {
        worker_ = std::thread([this, &index, next_state]() {
            if (next_state == MaintenanceState::kCompacting) {
                RunCompact(index);
            } else {
                RunIsolate(index);
            }
        });
    } catch (...) {
        state_.store(MaintenanceState::kIdle);
        idle_cv_.notify_all();
    }
}

MaintenanceState IndexMaintenance::State() const noexcept {
    return state_.load();
}

size_t IndexMaintenance::CompactRuns() const noexcept {
    return compact_runs_.load();
}

size_t IndexMaintenance::IsolateRuns() const noexcept {
    return isolate_runs_.load();
}

Result<void> IndexMaintenance::WaitForIdle(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    if (!idle_cv_.wait_until(lock, deadline, [this] {
            return state_.load() == MaintenanceState::kIdle;
        })) {
        return Result<void>::Err("等待 IndexMaintenance 空闲超时");
    }

    CleanupFinishedWorker(lock);
    return Result<void>::Ok();
}

void IndexMaintenance::RunCompact(VectorIndex& index) {
    auto compacted = index.Compact();
    compact_runs_.fetch_add(1);

    if (compacted) {
        LOG_INFO("INDEX_COMPACT_COMPLETED",
                 "size={}, total_slots={}, deleted={}",
                 index.Size(),
                 index.TotalSlots(),
                 index.DeletedCount());
    } else {
        LOG_WARN("INDEX_COMPACT_FAILED", "error={}", compacted.error);
    }

    {
        std::lock_guard lock(mutex_);
        if (compacted) {
            last_compact_time_ = std::chrono::steady_clock::now();
        }
        state_.store(MaintenanceState::kIdle);
    }
    idle_cv_.notify_all();
}

void IndexMaintenance::RunIsolate(VectorIndex& index) {
    auto isolated = index.Isolate();
    isolate_runs_.fetch_add(1);

    if (isolated) {
        LOG_INFO("INDEX_ISOLATE_COMPLETED",
                 "size={}, total_slots={}, deleted={}",
                 index.Size(),
                 index.TotalSlots(),
                 index.DeletedCount());
    } else {
        LOG_WARN("INDEX_ISOLATE_FAILED", "error={}", isolated.error);
    }

    Result<void> compacted = Result<void>::Err("isolate 未成功完成，跳过 compact");
    if (isolated) {
        state_.store(MaintenanceState::kCompacting);
        compacted = index.Compact();
        compact_runs_.fetch_add(1);
        if (compacted) {
            LOG_INFO("INDEX_COMPACT_COMPLETED",
                     "size={}, total_slots={}, deleted={}, source=isolate",
                     index.Size(),
                     index.TotalSlots(),
                     index.DeletedCount());
        } else {
            LOG_WARN("INDEX_COMPACT_FAILED", "error={}, source=isolate", compacted.error);
        }
    }

    {
        std::lock_guard lock(mutex_);
        if (compacted) {
            last_compact_time_ = std::chrono::steady_clock::now();
        }
        state_.store(MaintenanceState::kIdle);
    }
    idle_cv_.notify_all();
}

void IndexMaintenance::CleanupFinishedWorker(std::unique_lock<std::mutex>& lock) {
    if (!worker_.joinable() || state_.load() != MaintenanceState::kIdle) {
        return;
    }

    std::thread finished = std::move(worker_);
    lock.unlock();
    finished.join();
    lock.lock();
}
