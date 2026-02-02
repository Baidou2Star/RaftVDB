#pragma once

#include <string>
#include <utility>

// A tiny explicit error container used across the codebase. The design keeps
// success and failure states visible at call sites instead of relying on
// exceptions.
template <typename T>
struct [[nodiscard]] Result {
    T value{};
    bool ok = false;
    std::string error;

    explicit constexpr operator bool() const noexcept { return ok; }

    static Result Ok(T v) { return {std::move(v), true, {}}; }
    static Result Err(std::string msg) { return {T{}, false, std::move(msg)}; }

    T& operator*() noexcept { return value; }
    const T& operator*() const noexcept { return value; }

    T* operator->() noexcept { return &value; }
    const T* operator->() const noexcept { return &value; }
};

template <>
struct [[nodiscard]] Result<void> {
    bool ok = false;
    std::string error;

    explicit constexpr operator bool() const noexcept { return ok; }

    static Result Ok() { return {true, {}}; }
    static Result Err(std::string msg) { return {false, std::move(msg)}; }
};
