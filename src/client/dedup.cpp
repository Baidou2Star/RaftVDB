#include "client/dedup.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <utility>
#include <vector>

namespace {

Result<raftvdb::proto::DedupSnapshot> ParseSnapshot(const std::string& bytes) {
    raftvdb::proto::DedupSnapshot snapshot;
    if (!snapshot.ParseFromString(bytes)) {
        return Result<raftvdb::proto::DedupSnapshot>::Err("DedupSnapshot 反序列化失败");
    }
    return Result<raftvdb::proto::DedupSnapshot>::Ok(std::move(snapshot));
}

std::string BuildTempPath(const std::string& path) {
    return path + ".tmp";
}

} // namespace

DedupTable::DedupTable(uint32_t window_size) : window_size_(window_size) {}

DedupTable::CheckResult DedupTable::Check(const std::string& request_id,
                                          DedupEntry* out_entry) const {
    std::shared_lock lock(mutex_);
    auto found = table_.find(request_id);
    if (found == table_.end()) {
        return CheckResult::kNotFound;
    }

    if (out_entry != nullptr) {
        *out_entry = found->second;
    }
    return found->second.committed ? CheckResult::kAlreadyCommitted
                                   : CheckResult::kPendingCommit;
}

DedupTable::CheckResult DedupTable::TrackPending(const std::string& request_id,
                                                 uint64_t log_index,
                                                 DedupEntry* out_entry) {
    if (request_id.empty()) {
        if (out_entry != nullptr) {
            *out_entry = {};
        }
        return CheckResult::kNotFound;
    }

    std::unique_lock lock(mutex_);
    auto found = table_.find(request_id);
    if (found != table_.end()) {
        if (out_entry != nullptr) {
            *out_entry = found->second;
        }
        return found->second.committed ? CheckResult::kAlreadyCommitted
                                       : CheckResult::kPendingCommit;
    }

    DedupEntry entry;
    entry.committed = false;
    entry.success = false;
    entry.error.clear();
    entry.log_index = log_index;
    InsertOrUpdateLocked(request_id, entry);

    if (out_entry != nullptr) {
        *out_entry = entry;
    }
    return CheckResult::kNotFound;
}

void DedupTable::Record(const std::string& request_id,
                        uint64_t log_index,
                        bool success,
                        const std::string& error) {
    if (request_id.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);
    DedupEntry entry;
    entry.committed = true;
    entry.success = success;
    entry.error = error;
    entry.log_index = log_index;
    InsertOrUpdateLocked(request_id, entry);
}

Result<void> DedupTable::SaveTo(const std::string& path) const {
    std::shared_lock lock(mutex_);

    std::vector<std::pair<std::string, DedupEntry>> committed_entries;
    committed_entries.reserve(table_.size());
    for (const auto& [request_id, entry] : table_) {
        if (!entry.committed) {
            continue;
        }
        committed_entries.emplace_back(request_id, entry);
    }

    std::sort(committed_entries.begin(), committed_entries.end(),
              [](const auto& left, const auto& right) {
                  if (left.second.log_index == right.second.log_index) {
                      return left.first < right.first;
                  }
                  return left.second.log_index < right.second.log_index;
              });

    raftvdb::proto::DedupSnapshot snapshot;
    for (const auto& [request_id, entry] : committed_entries) {
        auto* record = snapshot.add_records();
        record->set_request_id(request_id);
        record->set_log_index(entry.log_index);
        record->set_success(entry.success);
        record->set_error(entry.error);
    }

    std::string bytes;
    if (!snapshot.SerializeToString(&bytes)) {
        return Result<void>::Err("DedupSnapshot 序列化失败");
    }

    std::filesystem::path output_path(path);
    std::error_code ec;
    if (output_path.has_parent_path()) {
        std::filesystem::create_directories(output_path.parent_path(), ec);
        if (ec) {
            return Result<void>::Err("创建 DedupTable 目录失败: " + ec.message());
        }
    }

    const std::string tmp_path = BuildTempPath(path);
    {
        std::ofstream output(tmp_path, std::ios::binary | std::ios::trunc);
        if (!output.is_open()) {
            return Result<void>::Err("打开 DedupTable 临时文件失败: " + tmp_path);
        }
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!output.good()) {
            return Result<void>::Err("写入 DedupTable 临时文件失败: " + tmp_path);
        }
    }

    std::filesystem::rename(tmp_path, output_path, ec);
    if (ec) {
        std::filesystem::remove(tmp_path, ec);
        return Result<void>::Err("原子替换 dedup.bin 失败: " + ec.message());
    }

    return Result<void>::Ok();
}

Result<void> DedupTable::LoadFrom(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        if (!std::filesystem::exists(path)) {
            std::unique_lock lock(mutex_);
            table_.clear();
            order_queue_.clear();
            return Result<void>::Ok();
        }
        return Result<void>::Err("打开 DedupTable 文件失败: " + path);
    }

    std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    auto parsed = ParseSnapshot(bytes);
    if (!parsed) {
        return Result<void>::Err(parsed.error);
    }

    std::vector<std::pair<std::string, DedupEntry>> entries;
    entries.reserve(static_cast<size_t>(parsed->records_size()));
    for (const auto& record : parsed->records()) {
        if (record.request_id().empty()) {
            return Result<void>::Err("DedupSnapshot 中存在空 request_id");
        }

        DedupEntry entry;
        entry.committed = true;
        entry.success = record.success();
        entry.error = record.error();
        entry.log_index = record.log_index();
        entries.emplace_back(record.request_id(), std::move(entry));
    }

    std::sort(entries.begin(), entries.end(),
              [](const auto& left, const auto& right) {
                  if (left.second.log_index == right.second.log_index) {
                      return left.first < right.first;
                  }
                  return left.second.log_index < right.second.log_index;
              });

    std::unique_lock lock(mutex_);
    table_.clear();
    order_queue_.clear();
    for (const auto& [request_id, entry] : entries) {
        InsertOrUpdateLocked(request_id, entry);
    }

    return Result<void>::Ok();
}

size_t DedupTable::Size() const {
    std::shared_lock lock(mutex_);
    return table_.size();
}

uint32_t DedupTable::WindowSize() const noexcept {
    return window_size_;
}

void DedupTable::InsertOrUpdateLocked(const std::string& request_id, const DedupEntry& entry) {
    table_[request_id] = entry;
    order_queue_.emplace_back(entry.log_index, request_id);
    EvictIfNeededLocked();
}

void DedupTable::EvictIfNeededLocked() {
    while (table_.size() > window_size_ && !order_queue_.empty()) {
        const auto [log_index, request_id] = order_queue_.front();
        order_queue_.pop_front();

        auto found = table_.find(request_id);
        if (found == table_.end()) {
            continue;
        }

        // 一个 request_id 可能先进入 pending，再在 Apply 后升级为 committed。
        // 队列里会留下旧的 log_index 快照，这里只删除“仍然对应当前记录”的最旧项。
        if (found->second.log_index != log_index) {
            continue;
        }

        table_.erase(found);
    }
}
