#pragma once

#include <cstddef>
#include <cstdint>

#include <deque>
#include <map>
#include <shared_mutex>
#include <string>

#include "common/result.hpp"
#include "raft.pb.h"

// DedupEntry 记录某个 request_id 当前在去重表中的状态。
// committed=false 表示请求已经进入复制流程但尚未 Apply；
// committed=true 表示请求已经提交完成，success/error 为最终结果。
struct DedupEntry {
    bool committed = false;
    bool success = false;
    std::string error;
    uint64_t log_index = 0;
};

class DedupTable {
public:
    enum class CheckResult { kNotFound, kPendingCommit, kAlreadyCommitted };

    explicit DedupTable(uint32_t window_size);

    // 读取 request_id 当前状态。
    // 1. 不存在：返回 kNotFound
    // 2. 已登记但未提交：返回 kPendingCommit
    // 3. 已提交：返回 kAlreadyCommitted，并可通过 out_entry 读取最终结果
    CheckResult Check(const std::string& request_id, DedupEntry* out_entry = nullptr) const;

    // 将请求登记为“已提出但未提交”。
    // 这是对技术文档主接口的一个补充：因为只靠 Check + Record 无法真正形成
    // kPendingCommit 状态，所以这里增加一个原子登记入口，供后续 Propose 路径复用。
    //
    // 返回值表示登记前的状态：
    // - kNotFound：本次成功插入 pending 记录
    // - kPendingCommit：该请求已在处理中
    // - kAlreadyCommitted：该请求已提交完成
    CheckResult TrackPending(const std::string& request_id,
                             uint64_t log_index,
                             DedupEntry* out_entry = nullptr);

    // 在 Apply 完成后记录最终执行结果。
    // 若该 request_id 之前已经以 pending 形式登记，会被原地升级为 committed；
    // 若不存在，也允许直接创建 committed 记录，便于恢复或重放路径复用。
    void Record(const std::string& request_id,
                uint64_t log_index,
                bool success,
                const std::string& error = "");

    // 将当前已提交记录保存为 protobuf 格式的 dedup.bin。
    // pending 记录不会被持久化，因为快照只代表已经 Apply 的状态机结果。
    Result<void> SaveTo(const std::string& path) const;

    // 从 dedup.bin 加载快照。
    // 若文件不存在，则按“空去重表”处理，便于首次启动和无快照启动场景。
    Result<void> LoadFrom(const std::string& path);

    // 调试与单测辅助接口。
    size_t Size() const;
    uint32_t WindowSize() const noexcept;

private:
    void InsertOrUpdateLocked(const std::string& request_id, const DedupEntry& entry);
    void EvictIfNeededLocked();

    uint32_t window_size_;
    std::map<std::string, DedupEntry> table_;
    std::deque<std::pair<uint64_t, std::string>> order_queue_;
    mutable std::shared_mutex mutex_;
};
