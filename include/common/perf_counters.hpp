#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <string>

// 轻量级全局性能计数器，用于定量分析热路径延迟。
// 所有字段均为 relaxed 原子写，读时 acquire，适合离线统计。

struct PerfCounters {
    // WAL fdatasync
    std::atomic<uint64_t> fdatasync_count{0};
    std::atomic<uint64_t> fdatasync_total_us{0};
    std::atomic<uint64_t> fdatasync_max_us{0};

    // HNSW 写锁等待
    std::atomic<uint64_t> hnsw_lock_wait_count{0};
    std::atomic<uint64_t> hnsw_lock_wait_total_us{0};
    std::atomic<uint64_t> hnsw_lock_wait_max_us{0};

    // HNSW index_.add() 执行
    std::atomic<uint64_t> hnsw_insert_count{0};
    std::atomic<uint64_t> hnsw_insert_total_us{0};
    std::atomic<uint64_t> hnsw_insert_max_us{0};

    // Apply 单条 entry（WAL 读 + HNSW upsert + 通知）
    std::atomic<uint64_t> apply_count{0};
    std::atomic<uint64_t> apply_total_us{0};
    std::atomic<uint64_t> apply_max_us{0};

    void UpdateMax(std::atomic<uint64_t>& field, uint64_t value) {
        uint64_t current = field.load(std::memory_order_relaxed);
        while (value > current &&
               !field.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
        }
    }

    void RecordFdatasync(uint64_t us) {
        fdatasync_count.fetch_add(1, std::memory_order_relaxed);
        fdatasync_total_us.fetch_add(us, std::memory_order_relaxed);
        UpdateMax(fdatasync_max_us, us);
    }

    void RecordHnswLockWait(uint64_t us) {
        hnsw_lock_wait_count.fetch_add(1, std::memory_order_relaxed);
        hnsw_lock_wait_total_us.fetch_add(us, std::memory_order_relaxed);
        UpdateMax(hnsw_lock_wait_max_us, us);
    }

    void RecordHnswInsert(uint64_t us) {
        hnsw_insert_count.fetch_add(1, std::memory_order_relaxed);
        hnsw_insert_total_us.fetch_add(us, std::memory_order_relaxed);
        UpdateMax(hnsw_insert_max_us, us);
    }

    void RecordApply(uint64_t us) {
        apply_count.fetch_add(1, std::memory_order_relaxed);
        apply_total_us.fetch_add(us, std::memory_order_relaxed);
        UpdateMax(apply_max_us, us);
    }

    void DumpToFile(const std::string& path) const {
        std::ofstream out(path);
        if (!out) return;

        auto mean = [](uint64_t total, uint64_t count) -> double {
            return count == 0 ? 0.0 : static_cast<double>(total) / static_cast<double>(count);
        };

        out << "metric,count,mean_us,max_us\n";
        out << "fdatasync,"
            << fdatasync_count.load() << ","
            << mean(fdatasync_total_us.load(), fdatasync_count.load()) << ","
            << fdatasync_max_us.load() << "\n";
        out << "hnsw_lock_wait,"
            << hnsw_lock_wait_count.load() << ","
            << mean(hnsw_lock_wait_total_us.load(), hnsw_lock_wait_count.load()) << ","
            << hnsw_lock_wait_max_us.load() << "\n";
        out << "hnsw_insert,"
            << hnsw_insert_count.load() << ","
            << mean(hnsw_insert_total_us.load(), hnsw_insert_count.load()) << ","
            << hnsw_insert_max_us.load() << "\n";
        out << "apply_per_entry,"
            << apply_count.load() << ","
            << mean(apply_total_us.load(), apply_count.load()) << ","
            << apply_max_us.load() << "\n";
    }
};

inline PerfCounters g_perf;

inline uint64_t NowUs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
