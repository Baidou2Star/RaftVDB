#include "raft/lease.hpp"

LeaseManager::LeaseManager() : expire_time_(std::chrono::steady_clock::time_point::min()) {}

void LeaseManager::Renew(std::chrono::milliseconds duration) {
    if (duration <= std::chrono::milliseconds::zero()) {
        Invalidate();
        return;
    }

    expire_time_.store(std::chrono::steady_clock::now() + duration,
                       std::memory_order_release);
}

bool LeaseManager::IsValid() const {
    const auto expire_time = expire_time_.load(std::memory_order_acquire);
    return std::chrono::steady_clock::now() < expire_time;
}

void LeaseManager::Invalidate() {
    expire_time_.store(std::chrono::steady_clock::time_point::min(),
                       std::memory_order_release);
}

std::chrono::steady_clock::time_point LeaseManager::ExpireTime() const {
    return expire_time_.load(std::memory_order_acquire);
}
