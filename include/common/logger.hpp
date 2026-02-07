#pragma once

#include <iosfwd>
#include <string>
#include <string_view>
#include <utility>

#include <spdlog/common.h>
#include <spdlog/fmt/fmt.h>

#include "common/result.hpp"

// Initializes the process-wide logger. Call this during startup after config
// loading so the level follows the configured environment.
Result<void> InitializeLogger(const std::string& level = "info");

// Test helper that redirects log output into a caller-provided stream.
Result<void> InitializeLoggerWithOstream(const std::string& level, std::ostream& stream);

Result<void> SetLogLevel(const std::string& level);
void FlushLogger();
void ShutdownLogger();

// The event-first logging helpers keep later Raft state transitions readable
// and consistent, e.g. LOG_INFO("ELECTED", "node_id={}, term={}", id, term).
void LogMessage(spdlog::level::level_enum level, std::string_view event, std::string message);

template <typename... Args>
void LogDebug(std::string_view event, fmt::format_string<Args...> format, Args&&... args) {
    LogMessage(spdlog::level::debug,
               event,
               fmt::format(format, std::forward<Args>(args)...));
}

template <typename... Args>
void LogInfo(std::string_view event, fmt::format_string<Args...> format, Args&&... args) {
    LogMessage(spdlog::level::info,
               event,
               fmt::format(format, std::forward<Args>(args)...));
}

template <typename... Args>
void LogWarn(std::string_view event, fmt::format_string<Args...> format, Args&&... args) {
    LogMessage(spdlog::level::warn,
               event,
               fmt::format(format, std::forward<Args>(args)...));
}

template <typename... Args>
void LogError(std::string_view event, fmt::format_string<Args...> format, Args&&... args) {
    LogMessage(spdlog::level::err,
               event,
               fmt::format(format, std::forward<Args>(args)...));
}

#define LOG_DEBUG(event, format, ...) ::LogDebug(event, format __VA_OPT__(,) __VA_ARGS__)
#define LOG_INFO(event, format, ...) ::LogInfo(event, format __VA_OPT__(,) __VA_ARGS__)
#define LOG_WARN(event, format, ...) ::LogWarn(event, format __VA_OPT__(,) __VA_ARGS__)
#define LOG_ERROR(event, format, ...) ::LogError(event, format __VA_OPT__(,) __VA_ARGS__)
