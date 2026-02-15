#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include <usearch/index_dense.hpp>

#include "common/config.hpp"
#include "common/result.hpp"

class VectorIndex {
public:
    struct SearchResult {
        uint64_t id = 0;
        float distance = 0.0F;
    };

    static Result<std::shared_ptr<VectorIndex>> Create(const VectorConfig& cfg);

    Result<void> Upsert(uint64_t id, const float* vec, size_t dim);
    Result<void> Delete(uint64_t id);
    Result<std::vector<SearchResult>> Search(const float* vec, size_t dim, size_t top_k) const;

    Result<void> Compact();
    Result<void> Isolate();

    size_t Size() const;
    size_t DeletedCount() const;
    size_t TotalSlots() const;

private:
    VectorIndex(unum::usearch::index_dense_t index, VectorConfig config);

    static Result<void> ValidateCreateConfig(const VectorConfig& cfg);
    static Result<void> ValidateVectorArgument(const float* vec,
                                              size_t dim,
                                              uint32_t expected_dim,
                                              std::string_view operation);
    static Result<unum::usearch::metric_kind_t> ParseMetric(std::string_view value);
    static Result<unum::usearch::scalar_kind_t> ParseScalarKind(std::string_view value);

    // USearch 本身支持并发访问，这里仍按设计文档在外层加读写锁，
    // 保证状态机写入与查询接口具备清晰的一致性边界。
    unum::usearch::index_dense_t index_;
    mutable std::shared_mutex mutex_;
    std::atomic<size_t> deleted_count_{0};
    VectorConfig config_;
};
