#include "storage/snapshot_store.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace {

std::string ToLower(std::string value) {
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string EscapeJson(std::string_view input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (char ch : input) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped += ch;
            break;
        }
    }
    return escaped;
}

Result<std::string> UnescapeJson(std::string_view input) {
    std::string value;
    value.reserve(input.size());
    for (size_t index = 0; index < input.size(); ++index) {
        const char ch = input[index];
        if (ch != '\\') {
            value += ch;
            continue;
        }
        if (index + 1 >= input.size()) {
            return Result<std::string>::Err("JSON 字符串转义不完整");
        }
        const char escaped = input[++index];
        switch (escaped) {
        case '\\':
            value += '\\';
            break;
        case '"':
            value += '"';
            break;
        case 'n':
            value += '\n';
            break;
        case 'r':
            value += '\r';
            break;
        case 't':
            value += '\t';
            break;
        default:
            return Result<std::string>::Err("JSON 包含不支持的转义字符");
        }
    }
    return Result<std::string>::Ok(std::move(value));
}

Result<size_t> FindJsonValueStart(const std::string& json, std::string_view key) {
    const std::string pattern = "\"" + std::string(key) + "\"";
    const size_t key_pos = json.find(pattern);
    if (key_pos == std::string::npos) {
        return Result<size_t>::Err("snapshot.meta 缺少字段: " + std::string(key));
    }

    const size_t colon_pos = json.find(':', key_pos + pattern.size());
    if (colon_pos == std::string::npos) {
        return Result<size_t>::Err("snapshot.meta 字段格式非法: " + std::string(key));
    }

    size_t value_pos = colon_pos + 1;
    while (value_pos < json.size() && std::isspace(static_cast<unsigned char>(json[value_pos]))) {
        ++value_pos;
    }
    if (value_pos >= json.size()) {
        return Result<size_t>::Err("snapshot.meta 字段值缺失: " + std::string(key));
    }
    return Result<size_t>::Ok(value_pos);
}

Result<std::string> ParseJsonString(const std::string& json, std::string_view key) {
    auto value_pos = FindJsonValueStart(json, key);
    if (!value_pos) {
        return Result<std::string>::Err(value_pos.error);
    }
    if (json[*value_pos] != '"') {
        return Result<std::string>::Err("snapshot.meta 字符串字段格式非法: " + std::string(key));
    }

    std::string raw;
    bool escaping = false;
    for (size_t index = *value_pos + 1; index < json.size(); ++index) {
        const char ch = json[index];
        if (!escaping && ch == '"') {
            return UnescapeJson(raw);
        }
        if (!escaping && ch == '\\') {
            escaping = true;
            raw += ch;
            continue;
        }
        escaping = false;
        raw += ch;
    }
    return Result<std::string>::Err("snapshot.meta 字符串未正确闭合: " + std::string(key));
}

template <typename UInt>
Result<UInt> ParseJsonUnsigned(const std::string& json, std::string_view key) {
    auto value_pos = FindJsonValueStart(json, key);
    if (!value_pos) {
        return Result<UInt>::Err(value_pos.error);
    }

    size_t end = *value_pos;
    while (end < json.size() && std::isdigit(static_cast<unsigned char>(json[end]))) {
        ++end;
    }
    if (end == *value_pos) {
        return Result<UInt>::Err("snapshot.meta 数字字段格式非法: " + std::string(key));
    }

    try {
        return Result<UInt>::Ok(static_cast<UInt>(std::stoull(json.substr(*value_pos, end - *value_pos))));
    } catch (const std::exception&) {
        return Result<UInt>::Err("snapshot.meta 数字字段超出范围: " + std::string(key));
    }
}

std::string CurrentUtcIso8601() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);

    std::tm utc_time{};
    gmtime_r(&now_time, &utc_time);

    std::ostringstream stream;
    stream << std::put_time(&utc_time, "%Y-%m-%dT%H:%M:%SZ");
    return stream.str();
}

Result<void> WriteTextFileAtomically(const std::string& path, const std::string& content) {
    const std::filesystem::path final_path(path);
    const std::filesystem::path tmp_path = final_path.string() + ".tmp";
    const std::filesystem::path parent_dir =
        final_path.has_parent_path() ? final_path.parent_path() : std::filesystem::current_path();

    std::error_code create_ec;
    std::filesystem::create_directories(parent_dir, create_ec);
    if (create_ec) {
        return Result<void>::Err("创建快照元数据目录失败: " + create_ec.message());
    }

    std::ofstream output(tmp_path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        return Result<void>::Err("创建快照元数据临时文件失败: " + tmp_path.string());
    }
    output << content;
    output.close();
    if (!output) {
        std::error_code ec;
        std::filesystem::remove(tmp_path, ec);
        return Result<void>::Err("写入快照元数据失败: " + tmp_path.string());
    }

    std::error_code rename_ec;
    std::filesystem::rename(tmp_path, final_path, rename_ec);
    if (rename_ec) {
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        return Result<void>::Err("原子替换快照元数据失败: " + rename_ec.message());
    }
    return Result<void>::Ok();
}

Result<std::string> ReadTextFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return Result<std::string>::Err("打开快照元数据失败: " + path);
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (!input.good() && !input.eof()) {
        return Result<std::string>::Err("读取快照元数据失败: " + path);
    }
    return Result<std::string>::Ok(buffer.str());
}

} // namespace

Result<void> SnapshotMeta::SaveToFile(const std::string& path) const {
    if (dim == 0) {
        return Result<void>::Err("snapshot.meta 的 dim 必须大于 0");
    }
    if (metric.empty()) {
        return Result<void>::Err("snapshot.meta 的 metric 不能为空");
    }
    if (data_type.empty()) {
        return Result<void>::Err("snapshot.meta 的 data_type 不能为空");
    }

    const std::string created = created_at.empty() ? CurrentUtcIso8601() : created_at;
    std::ostringstream json;
    json << "{\n"
         << "  \"raft_term\": " << raft_term << ",\n"
         << "  \"raft_index\": " << raft_index << ",\n"
         << "  \"dim\": " << dim << ",\n"
         << "  \"metric\": \"" << EscapeJson(metric) << "\",\n"
         << "  \"data_type\": \"" << EscapeJson(data_type) << "\",\n"
         << "  \"created_at\": \"" << EscapeJson(created) << "\"\n"
         << "}\n";

    return WriteTextFileAtomically(path, json.str());
}

Result<SnapshotMeta> SnapshotMeta::LoadFromFile(const std::string& path) {
    auto content = ReadTextFile(path);
    if (!content) {
        return Result<SnapshotMeta>::Err(content.error);
    }

    SnapshotMeta meta;
    auto raft_term = ParseJsonUnsigned<uint64_t>(*content, "raft_term");
    auto raft_index = ParseJsonUnsigned<uint64_t>(*content, "raft_index");
    auto dim = ParseJsonUnsigned<uint32_t>(*content, "dim");
    auto metric = ParseJsonString(*content, "metric");
    auto data_type = ParseJsonString(*content, "data_type");
    auto created_at = ParseJsonString(*content, "created_at");

    for (const auto* result : {static_cast<const Result<uint64_t>*>(&raft_term),
                               static_cast<const Result<uint64_t>*>(&raft_index)}) {
        if (!(*result)) {
            return Result<SnapshotMeta>::Err((*result).error);
        }
    }
    if (!dim) {
        return Result<SnapshotMeta>::Err(dim.error);
    }
    if (!metric) {
        return Result<SnapshotMeta>::Err(metric.error);
    }
    if (!data_type) {
        return Result<SnapshotMeta>::Err(data_type.error);
    }
    if (!created_at) {
        return Result<SnapshotMeta>::Err(created_at.error);
    }

    meta.raft_term = *raft_term;
    meta.raft_index = *raft_index;
    meta.dim = *dim;
    meta.metric = ToLower(*metric);
    meta.data_type = ToLower(*data_type);
    meta.created_at = *created_at;

    return Result<SnapshotMeta>::Ok(std::move(meta));
}

Result<void> SnapshotMeta::ValidateAgainstConfig(const VectorConfig& config) const {
    if (dim != config.dim) {
        return Result<void>::Err("snapshot.meta 与当前配置不一致: dim 不匹配");
    }
    if (ToLower(metric) != ToLower(config.metric)) {
        return Result<void>::Err("snapshot.meta 与当前配置不一致: metric 不匹配");
    }
    if (ToLower(data_type) != ToLower(config.data_type)) {
        return Result<void>::Err("snapshot.meta 与当前配置不一致: data_type 不匹配");
    }
    return Result<void>::Ok();
}

SnapshotStore::SnapshotStore(std::string snapshot_dir) : snapshot_dir_(std::move(snapshot_dir)) {}

Result<void> SnapshotStore::Initialize() const {
    try {
        std::filesystem::create_directories(snapshot_dir_);
    } catch (const std::exception& error) {
        return Result<void>::Err("创建快照目录失败: " + std::string(error.what()));
    }

    return CleanupTemporaryFiles();
}

Result<void> SnapshotStore::CleanupTemporaryFiles() const {
    try {
        for (const auto& entry : std::filesystem::directory_iterator(snapshot_dir_)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            if (!entry.path().filename().string().ends_with(".tmp")) {
                continue;
            }
            std::filesystem::remove(entry.path());
        }
        return Result<void>::Ok();
    } catch (const std::exception& error) {
        return Result<void>::Err("清理快照临时文件失败: " + std::string(error.what()));
    }
}

bool SnapshotStore::HasSnapshot() const {
    return std::filesystem::exists(SnapshotPath()) && std::filesystem::exists(MetaPath());
}

Result<SnapshotMeta> SnapshotStore::LoadLatest(const VectorConfig& config) const {
    if (!HasSnapshot()) {
        return Result<SnapshotMeta>::Err("当前没有可用的正式快照");
    }

    auto meta = SnapshotMeta::LoadFromFile(MetaPath());
    if (!meta) {
        return Result<SnapshotMeta>::Err(meta.error);
    }

    auto validate = meta->ValidateAgainstConfig(config);
    if (!validate) {
        return Result<SnapshotMeta>::Err(validate.error);
    }

    return meta;
}

std::string SnapshotStore::SnapshotPath() const {
    return (std::filesystem::path(snapshot_dir_) / "snapshot.usearch").string();
}

std::string SnapshotStore::MetaPath() const {
    return (std::filesystem::path(snapshot_dir_) / "snapshot.meta").string();
}

std::string SnapshotStore::TemporarySnapshotPath() const {
    return (std::filesystem::path(snapshot_dir_) / "snapshot.tmp").string();
}
