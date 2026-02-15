#include "vector/vector_index.hpp"

#include <algorithm>
#include <cctype>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <usearch/index_plugins.hpp>

namespace {

std::string ToLower(std::string value) {
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string FormatUsearchError(std::string_view prefix, const unum::usearch::error_t& error) {
    return std::string(prefix) + std::string(error.what());
}

size_t DetectThreadCount() {
    const unsigned int hardware_threads = std::thread::hardware_concurrency();
    return hardware_threads == 0U ? 1U : static_cast<size_t>(hardware_threads);
}

} // namespace

VectorIndex::VectorIndex(unum::usearch::index_dense_t index, VectorConfig config)
    : index_(std::move(index)), config_(std::move(config)) {}

Result<std::shared_ptr<VectorIndex>> VectorIndex::Create(const VectorConfig& cfg) {
    auto validate = ValidateCreateConfig(cfg);
    if (!validate) {
        return Result<std::shared_ptr<VectorIndex>>::Err(validate.error);
    }

    VectorConfig normalized = cfg;
    normalized.metric = ToLower(normalized.metric);
    normalized.data_type = ToLower(normalized.data_type);

    auto metric_kind = ParseMetric(normalized.metric);
    if (!metric_kind) {
        return Result<std::shared_ptr<VectorIndex>>::Err(metric_kind.error);
    }

    auto scalar_kind = ParseScalarKind(normalized.data_type);
    if (!scalar_kind) {
        return Result<std::shared_ptr<VectorIndex>>::Err(scalar_kind.error);
    }

    unum::usearch::index_dense_config_t index_config;
    index_config.connectivity = normalized.connectivity;
    index_config.connectivity_base = static_cast<size_t>(normalized.connectivity) * 2U;
    index_config.expansion_add = normalized.expansion_add;
    index_config.expansion_search = normalized.expansion_search;
    index_config.multi = false;
    index_config.enable_key_lookups = true;

    const unum::usearch::metric_punned_t metric(
        normalized.dim, *metric_kind, *scalar_kind);
    auto state = unum::usearch::index_dense_t::make(metric, index_config);
    if (!state) {
        return Result<std::shared_ptr<VectorIndex>>::Err(
            FormatUsearchError("创建 USearch 索引失败: ", state.error));
    }

    if (!state.index.try_reserve(
            unum::usearch::index_limits_t(normalized.initial_capacity, DetectThreadCount()))) {
        return Result<std::shared_ptr<VectorIndex>>::Err("预留 USearch 索引容量失败");
    }

    return Result<std::shared_ptr<VectorIndex>>::Ok(
        std::shared_ptr<VectorIndex>(new VectorIndex(std::move(state.index), std::move(normalized))));
}

Result<void> VectorIndex::Upsert(uint64_t id, const float* vec, size_t dim) {
    auto validate = ValidateVectorArgument(vec, dim, config_.dim, "Upsert");
    if (!validate) {
        return validate;
    }

    std::unique_lock lock(mutex_);

    size_t deleted = deleted_count_.load();
    auto removed = index_.remove(id);
    if (!removed) {
        return Result<void>::Err(FormatUsearchError("删除旧向量失败: ", removed.error));
    }
    deleted += removed.completed;

    // 追加写前先删旧值，维持“同一主键只保留最新向量”的覆盖语义。
    auto added = index_.add(id, vec);
    if (!added) {
        deleted_count_.store(deleted);
        return Result<void>::Err(FormatUsearchError("写入向量失败: ", added.error));
    }

    // 只要存在空闲槽位，USearch 的 add 就会复用其中一个槽位。
    if (deleted > 0U) {
        --deleted;
    }
    deleted_count_.store(deleted);
    return Result<void>::Ok();
}

Result<void> VectorIndex::Delete(uint64_t id) {
    std::unique_lock lock(mutex_);

    auto removed = index_.remove(id);
    if (!removed) {
        return Result<void>::Err(FormatUsearchError("删除向量失败: ", removed.error));
    }

    if (removed.completed > 0U) {
        deleted_count_.fetch_add(removed.completed);
    }
    return Result<void>::Ok();
}

Result<std::vector<VectorIndex::SearchResult>> VectorIndex::Search(const float* vec,
                                                                   size_t dim,
                                                                   size_t top_k) const {
    if (top_k == 0U) {
        return Result<std::vector<SearchResult>>::Ok({});
    }

    auto validate = ValidateVectorArgument(vec, dim, config_.dim, "Search");
    if (!validate) {
        return Result<std::vector<SearchResult>>::Err(validate.error);
    }

    std::shared_lock lock(mutex_);
    if (index_.size() == 0U) {
        return Result<std::vector<SearchResult>>::Ok({});
    }

    auto matches = index_.search(vec, top_k);
    if (!matches) {
        return Result<std::vector<SearchResult>>::Err(
            FormatUsearchError("检索向量失败: ", matches.error));
    }

    std::vector<SearchResult> results;
    results.reserve(matches.size());
    for (size_t index = 0; index < matches.size(); ++index) {
        const auto match = matches[index];
        results.push_back(
            SearchResult{static_cast<uint64_t>(match.member.key), static_cast<float>(match.distance)});
    }

    return Result<std::vector<SearchResult>>::Ok(std::move(results));
}

Result<void> VectorIndex::Compact() {
    std::unique_lock lock(mutex_);

    auto compacted = index_.compact();
    if (!compacted) {
        return Result<void>::Err(FormatUsearchError("压缩索引失败: ", compacted.error));
    }

    deleted_count_.store(0);
    return Result<void>::Ok();
}

Result<void> VectorIndex::Isolate() {
    std::unique_lock lock(mutex_);

    auto isolated = index_.isolate();
    if (!isolated) {
        return Result<void>::Err(FormatUsearchError("隔离游离边失败: ", isolated.error));
    }

    return Result<void>::Ok();
}

size_t VectorIndex::Size() const {
    std::shared_lock lock(mutex_);
    return index_.size();
}

size_t VectorIndex::DeletedCount() const {
    std::shared_lock lock(mutex_);
    return deleted_count_.load();
}

size_t VectorIndex::TotalSlots() const {
    std::shared_lock lock(mutex_);
    return index_.size() + deleted_count_.load();
}

Result<void> VectorIndex::ValidateCreateConfig(const VectorConfig& cfg) {
    if (cfg.dim == 0U) {
        return Result<void>::Err("vector.dim 必须大于 0");
    }
    if (cfg.initial_capacity == 0U) {
        return Result<void>::Err("vector.initial_capacity 必须大于 0");
    }
    if (cfg.connectivity < 2U) {
        return Result<void>::Err("vector.connectivity 必须大于等于 2");
    }
    if (cfg.expansion_add == 0U || cfg.expansion_search == 0U) {
        return Result<void>::Err("vector 扩展参数必须大于 0");
    }
    return Result<void>::Ok();
}

Result<void> VectorIndex::ValidateVectorArgument(const float* vec,
                                                 size_t dim,
                                                 uint32_t expected_dim,
                                                 std::string_view operation) {
    if (vec == nullptr) {
        return Result<void>::Err(std::string(operation) + " 的向量指针不能为空");
    }
    if (dim != expected_dim) {
        return Result<void>::Err(std::string(operation) + " 的向量维度与配置不一致");
    }
    return Result<void>::Ok();
}

Result<unum::usearch::metric_kind_t> VectorIndex::ParseMetric(std::string_view value) {
    const std::string normalized = ToLower(std::string(value));
    auto metric = unum::usearch::metric_from_name(normalized.c_str());
    if (!metric) {
        return Result<unum::usearch::metric_kind_t>::Err("不支持的 vector.metric: " + normalized);
    }
    return Result<unum::usearch::metric_kind_t>::Ok(*metric);
}

Result<unum::usearch::scalar_kind_t> VectorIndex::ParseScalarKind(std::string_view value) {
    const std::string normalized = ToLower(std::string(value));
    auto scalar = unum::usearch::scalar_kind_from_name(normalized.c_str());
    if (!scalar) {
        return Result<unum::usearch::scalar_kind_t>::Err("不支持的 vector.data_type: " + normalized);
    }
    return Result<unum::usearch::scalar_kind_t>::Ok(*scalar);
}
