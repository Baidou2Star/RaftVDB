#pragma once

#include <cstdint>
#include <string>

#include "common/result.hpp"

struct RaftMeta {
    uint64_t current_term = 0;
    std::string voted_for; // Empty means this node has not voted in the term yet.

    Result<void> Save(const std::string& path) const;
    static Result<RaftMeta> Load(const std::string& path);
};
