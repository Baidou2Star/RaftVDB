#include "storage/wal.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <iomanip>
#include <limits>
#include <sstream>
#include <system_error>
#include <unistd.h>

#include "common/logger.hpp"

namespace {

constexpr uint32_t kWalMagic = 0x31424452U; // Little-endian bytes spell "RDB1".
constexpr size_t kRecordHeaderSize = sizeof(uint32_t) * 3;

uint32_t ComputeCrc32(std::string_view bytes) {
    uint32_t crc = 0xFFFFFFFFU;
    for (unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = static_cast<uint32_t>(-(crc & 1U));
            crc = (crc >> 1U) ^ (0xEDB88320U & mask);
        }
    }
    return ~crc;
}

void EncodeUint32LE(uint32_t value, std::array<char, sizeof(uint32_t)>& out) {
    out[0] = static_cast<char>(value & 0xFFU);
    out[1] = static_cast<char>((value >> 8U) & 0xFFU);
    out[2] = static_cast<char>((value >> 16U) & 0xFFU);
    out[3] = static_cast<char>((value >> 24U) & 0xFFU);
}

uint32_t DecodeUint32LE(const char* bytes) {
    return static_cast<uint32_t>(static_cast<unsigned char>(bytes[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(bytes[1])) << 8U) |
           (static_cast<uint32_t>(static_cast<unsigned char>(bytes[2])) << 16U) |
           (static_cast<uint32_t>(static_cast<unsigned char>(bytes[3])) << 24U);
}

Result<void> WriteAll(int fd, const char* data, size_t size) {
    size_t written = 0;
    while (written < size) {
        const ssize_t rc = ::write(fd, data + written, size - written);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Result<void>::Err("WAL 写入失败: " + std::string(std::strerror(errno)));
        }
        written += static_cast<size_t>(rc);
    }
    return Result<void>::Ok();
}

Result<void> ReadExactly(int fd, uint64_t offset, char* buffer, size_t size, size_t* bytes_read) {
    *bytes_read = 0;
    while (*bytes_read < size) {
        const ssize_t rc =
            ::pread(fd, buffer + *bytes_read, size - *bytes_read, static_cast<off_t>(offset + *bytes_read));
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Result<void>::Err("WAL 读取失败: " + std::string(std::strerror(errno)));
        }
        if (rc == 0) {
            break;
        }
        *bytes_read += static_cast<size_t>(rc);
    }
    return Result<void>::Ok();
}

Result<int> OpenFile(const std::filesystem::path& path, int flags, mode_t mode = 0644) {
    const int fd = ::open(path.c_str(), flags, mode);
    if (fd < 0) {
        return Result<int>::Err("打开文件失败: " + path.string() + ", error=" +
                                std::string(std::strerror(errno)));
    }
    return Result<int>::Ok(fd);
}

Result<void> CloseFile(int* fd) {
    if (*fd < 0) {
        return Result<void>::Ok();
    }
    if (::close(*fd) != 0) {
        return Result<void>::Err("关闭文件失败: " + std::string(std::strerror(errno)));
    }
    *fd = -1;
    return Result<void>::Ok();
}

Result<void> TruncateFile(int fd, uint64_t size) {
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        return Result<void>::Err("截断文件失败: " + std::string(std::strerror(errno)));
    }
    return Result<void>::Ok();
}

std::string SegmentFileName(uint64_t start_index) {
    std::ostringstream name;
    name << "wal-" << std::setw(8) << std::setfill('0') << start_index << ".log";
    return name.str();
}

Result<uint64_t> ParseSegmentStartIndex(const std::filesystem::path& path) {
    const auto filename = path.filename().string();
    constexpr std::string_view prefix = "wal-";
    constexpr std::string_view suffix = ".log";
    if (!filename.starts_with(prefix) || !filename.ends_with(suffix)) {
        return Result<uint64_t>::Err("不是 WAL 段文件");
    }

    const auto number =
        filename.substr(prefix.size(), filename.size() - prefix.size() - suffix.size());
    try {
        return Result<uint64_t>::Ok(std::stoull(number));
    } catch (const std::exception&) {
        return Result<uint64_t>::Err("WAL 段文件名中的起始索引非法: " + filename);
    }
}

} // namespace

WAL::WAL(std::string dir, uint64_t start_index, uint64_t segment_size_bytes)
    : start_(start_index),
      dir_(std::move(dir)),
      segment_size_bytes_(segment_size_bytes) {}

Result<std::unique_ptr<WAL>> WAL::Open(const std::string& dir,
                                       uint64_t start_index,
                                       uint64_t segment_size_bytes) {
    if (segment_size_bytes == 0) {
        return Result<std::unique_ptr<WAL>>::Err("segment_size_bytes 必须大于 0");
    }

    try {
        std::filesystem::create_directories(dir);
    } catch (const std::exception& error) {
        return Result<std::unique_ptr<WAL>>::Err("创建 WAL 目录失败: " + std::string(error.what()));
    }

    auto wal = std::unique_ptr<WAL>(new WAL(dir, start_index, segment_size_bytes));
    auto load_result = wal->LoadExistingSegments();
    if (!load_result) {
        return Result<std::unique_ptr<WAL>>::Err(load_result.error);
    }

    auto open_result = wal->OpenActiveSegmentForAppend();
    if (!open_result) {
        return Result<std::unique_ptr<WAL>>::Err(open_result.error);
    }

    return Result<std::unique_ptr<WAL>>::Ok(std::move(wal));
}

Result<void> WAL::LoadExistingSegments() {
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        auto start_index = ParseSegmentStartIndex(entry.path());
        if (!start_index) {
            continue;
        }

        SegmentMeta segment;
        segment.start_index = *start_index;
        segment.path = entry.path();
        segment.size_bytes = static_cast<uint64_t>(entry.file_size());
        segments_.push_back(std::move(segment));
    }

    std::sort(segments_.begin(), segments_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.start_index < rhs.start_index;
    });

    if (segments_.empty()) {
        auto create_result = CreateFreshSegment(start_);
        if (!create_result) {
            return create_result;
        }
        return Result<void>::Ok();
    }

    for (size_t index = 0; index < segments_.size(); ++index) {
        bool truncated = false;
        auto recover_result = RecoverSegment(index, &truncated);
        if (!recover_result) {
            return recover_result;
        }
        if (truncated) {
            auto remove_result = RemoveSegmentsAfter(index);
            if (!remove_result) {
                return remove_result;
            }
            break;
        }
    }

    if (segments_.empty()) {
        auto create_result = CreateFreshSegment(start_);
        if (!create_result) {
            return create_result;
        }
    }

    start_ = segments_.front().start_index;
    return Result<void>::Ok();
}

Result<void> WAL::OpenActiveSegmentForAppend() {
    auto close_result = CloseFile(&fd_);
    if (!close_result) {
        return close_result;
    }
    if (segments_.empty()) {
        return Result<void>::Err("没有可用的 WAL 段文件");
    }

    auto open_result = OpenFile(segments_.back().path, O_RDWR | O_CREAT, 0644);
    if (!open_result) {
        return Result<void>::Err(open_result.error);
    }
    fd_ = *open_result;

    // 重新打开已有活动段时，必须把写指针显式移动到当前段尾。
    // 否则在恢复、截断或重启后继续追加时，新的 WAL 记录会从文件头覆盖旧数据。
    if (::lseek(fd_, static_cast<off_t>(segments_.back().size_bytes), SEEK_SET) < 0) {
        auto error = std::string(std::strerror(errno));
        auto ignored = CloseFile(&fd_);
        (void)ignored;
        return Result<void>::Err("定位 WAL 活动段写指针失败: " + error);
    }
    return Result<void>::Ok();
}

Result<void> WAL::CreateFreshSegment(uint64_t start_index) {
    SegmentMeta segment;
    segment.start_index = start_index;
    segment.path = std::filesystem::path(dir_) / SegmentFileName(start_index);
    segment.last_index = start_index > 0 ? start_index - 1 : 0;
    segment.size_bytes = 0;

    auto open_result = OpenFile(segment.path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (!open_result) {
        return Result<void>::Err(open_result.error);
    }

    int temp_fd = *open_result;
    auto close_result = CloseFile(&temp_fd);
    if (!close_result) {
        return close_result;
    }

    segments_.push_back(std::move(segment));
    return Result<void>::Ok();
}

Result<void> WAL::RotateSegment(uint64_t next_index) {
    auto close_result = CloseFile(&fd_);
    if (!close_result) {
        return close_result;
    }

    auto create_result = CreateFreshSegment(next_index);
    if (!create_result) {
        return create_result;
    }

    return OpenActiveSegmentForAppend();
}

Result<void> WAL::EnsureActiveSegmentForAppend(uint64_t next_index, size_t record_size) {
    if (segments_.empty()) {
        auto create_result = CreateFreshSegment(next_index);
        if (!create_result) {
            return create_result;
        }
    }

    auto& active = segments_.back();
    if (active.size_bytes != 0 && active.size_bytes + record_size > segment_size_bytes_) {
        return RotateSegment(next_index);
    }
    if (fd_ < 0) {
        return OpenActiveSegmentForAppend();
    }
    return Result<void>::Ok();
}

Result<void> WAL::Append(const LogEntry& entry) {
    if (!offset_index_.empty() && entry.index != last_index_ + 1) {
        return Result<void>::Err("WAL 追加的日志索引不连续");
    }

    const std::string payload = entry.Serialize();
    if (payload.empty()) {
        return Result<void>::Err("WAL 序列化日志失败");
    }
    if (payload.size() > std::numeric_limits<uint32_t>::max()) {
        return Result<void>::Err("WAL 记录过大");
    }

    const uint32_t length = static_cast<uint32_t>(payload.size());
    const uint32_t crc = ComputeCrc32(payload);
    const size_t record_size = kRecordHeaderSize + payload.size();

    auto ensure_result = EnsureActiveSegmentForAppend(entry.index, record_size);
    if (!ensure_result) {
        return ensure_result;
    }

    auto& active = segments_.back();
    const uint64_t offset = active.size_bytes;

    std::array<char, sizeof(uint32_t)> magic_bytes{};
    std::array<char, sizeof(uint32_t)> length_bytes{};
    std::array<char, sizeof(uint32_t)> crc_bytes{};
    EncodeUint32LE(kWalMagic, magic_bytes);
    EncodeUint32LE(length, length_bytes);
    EncodeUint32LE(crc, crc_bytes);

    auto write_magic = WriteAll(fd_, magic_bytes.data(), magic_bytes.size());
    if (!write_magic) {
        return write_magic;
    }
    auto write_length = WriteAll(fd_, length_bytes.data(), length_bytes.size());
    if (!write_length) {
        return write_length;
    }
    auto write_crc = WriteAll(fd_, crc_bytes.data(), crc_bytes.size());
    if (!write_crc) {
        return write_crc;
    }
    auto write_payload = WriteAll(fd_, payload.data(), payload.size());
    if (!write_payload) {
        return write_payload;
    }

    active.size_bytes += record_size;
    active.last_index = entry.index;
    offset_index_[entry.index] = EntryLocation{active.path, offset};
    last_index_ = entry.index;
    return Result<void>::Ok();
}

Result<void> WAL::Flush() {
    if (fd_ < 0) {
        return Result<void>::Err("WAL 未打开");
    }
    if (::fdatasync(fd_) != 0) {
        LOG_ERROR("WAL_SYNC_FAILED", "path={}, error={}", segments_.back().path.string(),
                  std::strerror(errno));
        return Result<void>::Err("fdatasync 失败: " + std::string(std::strerror(errno)));
    }
    return Result<void>::Ok();
}

Result<LogEntry> WAL::ReadRecordAt(const EntryLocation& location) const {
    int fd = -1;
    if (fd_ >= 0 && !segments_.empty() && location.path == segments_.back().path) {
        fd = fd_;
    } else {
        auto open_result = OpenFile(location.path, O_RDONLY);
        if (!open_result) {
            return Result<LogEntry>::Err(open_result.error);
        }
        fd = *open_result;
    }

    std::array<char, kRecordHeaderSize> header{};
    size_t bytes_read = 0;
    auto read_header = ReadExactly(fd, location.offset, header.data(), header.size(), &bytes_read);
    if (fd != fd_) {
        auto ignored = CloseFile(&fd);
        (void)ignored;
    }
    if (!read_header) {
        return Result<LogEntry>::Err(read_header.error);
    }
    if (bytes_read != header.size()) {
        return Result<LogEntry>::Err("WAL 记录头不完整");
    }

    const uint32_t magic = DecodeUint32LE(header.data());
    const uint32_t length = DecodeUint32LE(header.data() + sizeof(uint32_t));
    const uint32_t crc = DecodeUint32LE(header.data() + sizeof(uint32_t) * 2);
    if (magic != kWalMagic) {
        return Result<LogEntry>::Err("WAL magic 非法");
    }

    std::string payload(length, '\0');
    if (fd_ >= 0 && !segments_.empty() && location.path == segments_.back().path) {
        fd = fd_;
    } else {
        auto reopen_result = OpenFile(location.path, O_RDONLY);
        if (!reopen_result) {
            return Result<LogEntry>::Err(reopen_result.error);
        }
        fd = *reopen_result;
    }
    bytes_read = 0;
    auto read_payload =
        ReadExactly(fd, location.offset + kRecordHeaderSize, payload.data(), payload.size(), &bytes_read);
    if (fd != fd_) {
        auto ignored = CloseFile(&fd);
        (void)ignored;
    }
    if (!read_payload) {
        return Result<LogEntry>::Err(read_payload.error);
    }
    if (bytes_read != payload.size()) {
        return Result<LogEntry>::Err("WAL payload 不完整");
    }
    if (ComputeCrc32(payload) != crc) {
        return Result<LogEntry>::Err("WAL CRC 校验失败");
    }

    return LogEntry::Deserialize(payload);
}

Result<LogEntry> WAL::Read(uint64_t index) {
    const auto found = offset_index_.find(index);
    if (found == offset_index_.end()) {
        return Result<LogEntry>::Err("日志索引不存在: " + std::to_string(index));
    }
    return ReadRecordAt(found->second);
}

Result<std::vector<LogEntry>> WAL::ReadFrom(uint64_t from_index) {
    return ReadFrom(from_index, std::numeric_limits<size_t>::max());
}

Result<std::vector<LogEntry>> WAL::ReadFrom(uint64_t from_index, size_t max_entries) {
    std::vector<LogEntry> entries;
    if (max_entries == 0 || offset_index_.empty() || from_index > last_index_) {
        return Result<std::vector<LogEntry>>::Ok(std::move(entries));
    }

    const auto begin = offset_index_.lower_bound(from_index);
    if (begin == offset_index_.end() || begin->first != from_index) {
        return Result<std::vector<LogEntry>>::Err("起始日志索引不存在: " + std::to_string(from_index));
    }

    for (auto it = begin; it != offset_index_.end(); ++it) {
        if (entries.size() >= max_entries) {
            break;
        }
        auto entry = ReadRecordAt(it->second);
        if (!entry) {
            return Result<std::vector<LogEntry>>::Err(entry.error);
        }
        entries.push_back(std::move(*entry));
    }

    return Result<std::vector<LogEntry>>::Ok(std::move(entries));
}

Result<uint64_t> WAL::TermAt(uint64_t index) {
    if (index == 0) {
        return Result<uint64_t>::Ok(0);
    }

    auto entry = Read(index);
    if (!entry) {
        return Result<uint64_t>::Err(entry.error);
    }
    return Result<uint64_t>::Ok(entry->term);
}

Result<void> WAL::RemoveSegmentEntries(const SegmentMeta& segment) {
    for (auto it = offset_index_.begin(); it != offset_index_.end();) {
        if (it->second.path == segment.path) {
            it = offset_index_.erase(it);
        } else {
            ++it;
        }
    }
    return Result<void>::Ok();
}

Result<void> WAL::RemoveSegmentsAfter(size_t segment_index) {
    if (segment_index + 1 >= segments_.size()) {
        return Result<void>::Ok();
    }

    for (size_t index = segment_index + 1; index < segments_.size(); ++index) {
        auto remove_entries = RemoveSegmentEntries(segments_[index]);
        if (!remove_entries) {
            return remove_entries;
        }
        std::error_code ec;
        std::filesystem::remove(segments_[index].path, ec);
        if (ec) {
            return Result<void>::Err("删除损坏后的 WAL 段失败: " + ec.message());
        }
    }
    segments_.erase(segments_.begin() + static_cast<std::ptrdiff_t>(segment_index + 1), segments_.end());
    return Result<void>::Ok();
}

Result<void> WAL::RecoverSegment(size_t segment_index, bool* truncated) {
    *truncated = false;
    auto& segment = segments_[segment_index];

    auto open_result = OpenFile(segment.path, O_RDWR);
    if (!open_result) {
        return Result<void>::Err(open_result.error);
    }
    int fd = *open_result;
    uint64_t offset = 0;
    uint64_t last_index = segment.start_index > 0 ? segment.start_index - 1 : 0;

    while (offset < segment.size_bytes) {
        std::array<char, kRecordHeaderSize> header{};
        size_t bytes_read = 0;
        auto read_header = ReadExactly(fd, offset, header.data(), header.size(), &bytes_read);
        if (!read_header) {
            auto ignored = CloseFile(&fd);
            (void)ignored;
            return read_header;
        }
        if (bytes_read == 0) {
            break;
        }
        if (bytes_read != header.size()) {
            auto truncate_result = TruncateFile(fd, offset);
            auto ignored = CloseFile(&fd);
            (void)ignored;
            if (!truncate_result) {
                return truncate_result;
            }
            LOG_WARN("WAL_RECOVER_TRUNCATE", "path={}, reason=incomplete header, offset={}",
                     segment.path.string(), offset);
            segment.size_bytes = offset;
            *truncated = true;
            break;
        }

        const uint32_t magic = DecodeUint32LE(header.data());
        const uint32_t length = DecodeUint32LE(header.data() + sizeof(uint32_t));
        const uint32_t crc = DecodeUint32LE(header.data() + sizeof(uint32_t) * 2);
        if (magic != kWalMagic) {
            auto truncate_result = TruncateFile(fd, offset);
            auto ignored = CloseFile(&fd);
            (void)ignored;
            if (!truncate_result) {
                return truncate_result;
            }
            LOG_WARN("WAL_RECOVER_TRUNCATE", "path={}, reason=invalid magic, offset={}",
                     segment.path.string(), offset);
            segment.size_bytes = offset;
            *truncated = true;
            break;
        }

        std::string payload(length, '\0');
        bytes_read = 0;
        auto read_payload = ReadExactly(fd, offset + kRecordHeaderSize, payload.data(), payload.size(), &bytes_read);
        if (!read_payload) {
            auto ignored = CloseFile(&fd);
            (void)ignored;
            return read_payload;
        }
        if (bytes_read != payload.size() || ComputeCrc32(payload) != crc) {
            auto truncate_result = TruncateFile(fd, offset);
            auto ignored = CloseFile(&fd);
            (void)ignored;
            if (!truncate_result) {
                return truncate_result;
            }
            LOG_WARN("WAL_RECOVER_TRUNCATE", "path={}, reason=crc or incomplete payload, offset={}",
                     segment.path.string(), offset);
            segment.size_bytes = offset;
            *truncated = true;
            break;
        }

        auto entry = LogEntry::Deserialize(payload);
        if (!entry) {
            auto truncate_result = TruncateFile(fd, offset);
            auto ignored = CloseFile(&fd);
            (void)ignored;
            if (!truncate_result) {
                return truncate_result;
            }
            LOG_WARN("WAL_RECOVER_TRUNCATE", "path={}, reason=invalid log entry, offset={}",
                     segment.path.string(), offset);
            segment.size_bytes = offset;
            *truncated = true;
            break;
        }

        offset_index_[entry->index] = EntryLocation{segment.path, offset};
        last_index = entry->index;
        offset += kRecordHeaderSize + payload.size();
    }

    auto close_result = CloseFile(&fd);
    if (!close_result) {
        return close_result;
    }

    segment.last_index = last_index;
    segment.size_bytes = offset;
    last_index_ = std::max(last_index_, last_index);
    return Result<void>::Ok();
}

Result<void> WAL::TruncateBefore(uint64_t index) {
    if (segments_.empty()) {
        return Result<void>::Ok();
    }

    bool removed_any = false;
    for (auto it = segments_.begin(); it != segments_.end();) {
        if (it->last_index != 0 && it->last_index < index) {
            removed_any = true;
            auto remove_entries = RemoveSegmentEntries(*it);
            if (!remove_entries) {
                return remove_entries;
            }

            const bool removing_active = (it == segments_.end() - 1);
            if (removing_active) {
                auto close_result = CloseFile(&fd_);
                if (!close_result) {
                    return close_result;
                }
            }

            std::error_code ec;
            std::filesystem::remove(it->path, ec);
            if (ec) {
                return Result<void>::Err("删除 WAL 段失败: " + ec.message());
            }

            it = segments_.erase(it);
        } else {
            ++it;
        }
    }

    if (offset_index_.empty()) {
        last_index_ = 0;
    } else {
        last_index_ = offset_index_.rbegin()->first;
    }

    if (segments_.empty()) {
        auto create_result = CreateFreshSegment(index);
        if (!create_result) {
            return create_result;
        }
    }

    start_ = segments_.front().start_index;

    if (removed_any || fd_ < 0) {
        auto open_result = OpenActiveSegmentForAppend();
        if (!open_result) {
            return open_result;
        }
    }

    return Result<void>::Ok();
}

Result<void> WAL::TruncateSuffix(uint64_t index) {
    if (index == 0) {
        return Result<void>::Err("TruncateSuffix 失败: index 必须从 1 开始");
    }
    if (offset_index_.empty() || index > last_index_) {
        return Result<void>::Ok();
    }

    auto close_result = CloseFile(&fd_);
    if (!close_result) {
        return close_result;
    }

    auto remove_it = offset_index_.lower_bound(index);
    if (remove_it == offset_index_.end()) {
        auto reopen = OpenActiveSegmentForAppend();
        if (!reopen) {
            return reopen;
        }
        return Result<void>::Ok();
    }

    const auto truncate_location = remove_it->second;
    bool truncated_partial_segment = false;
    std::filesystem::path partial_segment_path;

    for (auto it = segments_.begin(); it != segments_.end();) {
        if (it->start_index >= index) {
            std::error_code ec;
            std::filesystem::remove(it->path, ec);
            if (ec) {
                return Result<void>::Err("删除 WAL 尾部段失败: " + ec.message());
            }
            it = segments_.erase(it);
            continue;
        }

        if (it->last_index >= index) {
            auto open_result = OpenFile(it->path, O_RDWR);
            if (!open_result) {
                return Result<void>::Err(open_result.error);
            }
            int fd = *open_result;
            auto truncate_result = TruncateFile(fd, truncate_location.offset);
            auto close_segment = CloseFile(&fd);
            if (!truncate_result) {
                (void)close_segment;
                return truncate_result;
            }
            if (!close_segment) {
                return close_segment;
            }

            it->size_bytes = truncate_location.offset;
            it->last_index = index > it->start_index ? index - 1 : 0;
            truncated_partial_segment = true;
            partial_segment_path = it->path;

            ++it;
            while (it != segments_.end()) {
                std::error_code ec;
                std::filesystem::remove(it->path, ec);
                if (ec) {
                    return Result<void>::Err("删除 WAL 尾部段失败: " + ec.message());
                }
                it = segments_.erase(it);
            }
            break;
        }

        ++it;
    }

    for (auto it = offset_index_.begin(); it != offset_index_.end();) {
        if (it->first >= index) {
            it = offset_index_.erase(it);
        } else {
            ++it;
        }
    }

    if (segments_.empty()) {
        auto create_result = CreateFreshSegment(index);
        if (!create_result) {
            return create_result;
        }
    } else if (truncated_partial_segment && segments_.back().path != partial_segment_path) {
        auto remove_result = RemoveSegmentsAfter(segments_.size() - 1);
        if (!remove_result) {
            return remove_result;
        }
    }

    start_ = segments_.front().start_index;
    last_index_ = offset_index_.empty() ? index - 1 : offset_index_.rbegin()->first;

    auto reopen_result = OpenActiveSegmentForAppend();
    if (!reopen_result) {
        return reopen_result;
    }
    return Result<void>::Ok();
}

Result<void> WAL::Close() {
    return CloseFile(&fd_);
}
