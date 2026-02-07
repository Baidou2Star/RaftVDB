#include "common/logger.hpp"

#include <algorithm>
#include <cctype>
#include <memory>
#include <mutex>

#include <spdlog/logger.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace {

std::mutex& LoggerMutex() {
    static std::mutex mutex;
    return mutex;
}

std::shared_ptr<spdlog::logger>& GlobalLogger() {
    static std::shared_ptr<spdlog::logger> logger;
    return logger;
}

std::string ToLower(std::string value) {
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

Result<spdlog::level::level_enum> ParseLogLevel(const std::string& level) {
    const auto normalized = ToLower(level);
    if (normalized == "trace") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::trace);
    }
    if (normalized == "debug") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::debug);
    }
    if (normalized == "info") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::info);
    }
    if (normalized == "warn" || normalized == "warning") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::warn);
    }
    if (normalized == "error" || normalized == "err") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::err);
    }
    if (normalized == "critical") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::critical);
    }
    if (normalized == "off") {
        return Result<spdlog::level::level_enum>::Ok(spdlog::level::off);
    }

    return Result<spdlog::level::level_enum>::Err(
        "不支持的日志级别: " + level +
        "，仅支持 trace/debug/info/warn/error/critical/off");
}

std::shared_ptr<spdlog::logger> BuildLogger(
    spdlog::level::level_enum level,
    std::shared_ptr<spdlog::sinks::sink> sink) {
    auto logger = std::make_shared<spdlog::logger>("raftvdb", std::move(sink));
    logger->set_level(level);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");
    return logger;
}

std::shared_ptr<spdlog::logger> GetOrCreateLogger() {
    std::lock_guard<std::mutex> lock(LoggerMutex());
    if (!GlobalLogger()) {
        auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        GlobalLogger() = BuildLogger(spdlog::level::info, std::move(sink));
    }
    return GlobalLogger();
}

} // namespace

Result<void> InitializeLogger(const std::string& level) {
    auto parsed_level = ParseLogLevel(level);
    if (!parsed_level) {
        return Result<void>::Err(parsed_level.error);
    }

    std::lock_guard<std::mutex> lock(LoggerMutex());
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    GlobalLogger() = BuildLogger(*parsed_level, std::move(sink));
    return Result<void>::Ok();
}

Result<void> InitializeLoggerWithOstream(const std::string& level, std::ostream& stream) {
    auto parsed_level = ParseLogLevel(level);
    if (!parsed_level) {
        return Result<void>::Err(parsed_level.error);
    }

    std::lock_guard<std::mutex> lock(LoggerMutex());
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream);
    GlobalLogger() = BuildLogger(*parsed_level, std::move(sink));
    return Result<void>::Ok();
}

Result<void> SetLogLevel(const std::string& level) {
    auto parsed_level = ParseLogLevel(level);
    if (!parsed_level) {
        return Result<void>::Err(parsed_level.error);
    }

    auto logger = GetOrCreateLogger();
    logger->set_level(*parsed_level);
    return Result<void>::Ok();
}

void FlushLogger() {
    GetOrCreateLogger()->flush();
}

void ShutdownLogger() {
    std::lock_guard<std::mutex> lock(LoggerMutex());
    if (GlobalLogger()) {
        GlobalLogger()->flush();
        GlobalLogger().reset();
    }
}

void LogMessage(spdlog::level::level_enum level, std::string_view event, std::string message) {
    auto logger = GetOrCreateLogger();
    logger->log(level, "event={} {}", event, message);
}
