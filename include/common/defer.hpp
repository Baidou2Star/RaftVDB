#pragma once

#include <functional>
#include <utility>

// Defer is a scope guard for cleanup paths that should always run when the
// current scope exits, including early returns.
class Defer {
public:
    explicit Defer(std::function<void()> f) noexcept : fn_(std::move(f)) {}

    ~Defer() noexcept {
        if (fn_) {
            fn_();
        }
    }

    Defer(const Defer&) = delete;
    Defer& operator=(const Defer&) = delete;

private:
    std::function<void()> fn_;
};
