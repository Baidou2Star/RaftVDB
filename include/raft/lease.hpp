#pragma once

#include <atomic>
#include <chrono>

// LeaseManager 负责维护 Leader 的本地租约状态。
// 设计上它故意保持很薄，只负责“一个到期时间”的线程安全读写：
// 1. Leader 心跳拿到 Quorum 后调用 Renew() 续约
// 2. 读请求前调用 IsValid() 判断是否还能走租约读
// 3. 降级为 Follower 时调用 Invalidate() 主动失效
//
// 这里强制使用 steady_clock，避免系统时间调整造成租约误判。
class LeaseManager {
public:
    LeaseManager();

    // 续约到“当前时间 + duration”。
    // 若 duration <= 0，则视为一次无效续约，直接把租约清空，
    // 防止上层误传 0 或负值后仍保留旧租约。
    void Renew(std::chrono::milliseconds duration);

    // 判断租约当前是否仍然有效。
    // 只有当 now < expire_time_ 时返回 true，等于到期点时视为已过期，
    // 这样边界更保守，更符合线性一致性读的安全要求。
    bool IsValid() const;

    // 主动失效租约，通常在 BecomeFollower 时调用。
    void Invalidate();

    // 返回当前记录的到期时间，便于日志和调试观察。
    std::chrono::steady_clock::time_point ExpireTime() const;

private:
    // 原子保存到期时间，便于并发场景下无锁读取。
    std::atomic<std::chrono::steady_clock::time_point> expire_time_;
};
