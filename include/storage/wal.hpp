#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>
#include <memory>
#include <string>
#include <vector>

#include "common/result.hpp"
#include "raft/log_entry.hpp"

class WAL {
public:
    static constexpr uint64_t kDefaultSegmentSizeBytes = 64ULL * 1024ULL * 1024ULL;

    static Result<std::unique_ptr<WAL>> Open(const std::string& dir,
                                             uint64_t start_index,
                                             uint64_t segment_size_bytes = kDefaultSegmentSizeBytes);

    Result<void> Append(const LogEntry& entry);
    Result<void> Flush();                              // fdatasync
    Result<LogEntry> Read(uint64_t index);
    Result<std::vector<LogEntry>> ReadFrom(uint64_t from_index);
    Result<std::vector<LogEntry>> ReadFrom(uint64_t from_index, size_t max_entries);
    Result<uint64_t> TermAt(uint64_t index);
    Result<void> TruncateBefore(uint64_t index);      // 快照后截断整段旧日志
    Result<void> TruncateSuffix(uint64_t index);      // 冲突覆盖时裁掉 index 及之后的尾部日志
    uint64_t LastIndex() const;
    Result<void> Close();

private:
    struct EntryLocation {
        std::filesystem::path path;
        uint64_t offset = 0;
    };

    struct SegmentMeta {
        uint64_t start_index = 0;
        uint64_t last_index = 0;
        uint64_t size_bytes = 0;
        std::filesystem::path path;
    };

    WAL(std::string dir, uint64_t start_index, uint64_t segment_size_bytes);

    Result<void> LoadExistingSegments();
    Result<void> OpenActiveSegmentForAppend();
    Result<void> RotateSegment(uint64_t next_index);
    Result<void> EnsureActiveSegmentForAppend(uint64_t next_index, size_t record_size);
    Result<void> RecoverSegment(size_t segment_index, bool* truncated);
    Result<void> RebuildIndexForSegment(const SegmentMeta& segment);
    Result<void> RemoveSegmentsAfter(size_t segment_index);
    Result<void> RemoveSegmentEntries(const SegmentMeta& segment);
    Result<void> CreateFreshSegment(uint64_t start_index);
    // WAL 的公开接口会被复制线程、Apply 线程和快照线程并发调用。
    // 这些内部辅助函数默认要求调用方已经持有 mutex_，从而保证段元数据、
    // 活动文件描述符和内存索引始终一致。
    Result<LogEntry> ReadRecordAtUnlocked(const EntryLocation& location) const;
    Result<LogEntry> ReadUnlocked(uint64_t index) const;
    Result<std::vector<LogEntry>> ReadFromUnlocked(uint64_t from_index, size_t max_entries) const;
    Result<uint64_t> TermAtUnlocked(uint64_t index) const;

    int fd_ = -1;
    uint64_t start_ = 0;
    uint64_t last_index_ = 0;
    std::string dir_;
    uint64_t segment_size_bytes_ = kDefaultSegmentSizeBytes;
    std::map<uint64_t, EntryLocation> offset_index_;
    std::vector<SegmentMeta> segments_;
    mutable std::mutex mutex_;
};
