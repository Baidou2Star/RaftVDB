#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/result.hpp"

enum class EntryType : uint8_t {
    kNormal = 1,
    kConfig = 2,
    kNoop = 3,
};

enum class CmdType : uint8_t {
    kUpsert = 1,
    kDelete = 2,
};

struct UpsertCmd {
    uint64_t id = 0;
    std::vector<float> vector;
    std::string request_id;

    std::string Serialize() const;
    static Result<UpsertCmd> Deserialize(std::string_view data);
};

struct DeleteCmd {
    uint64_t id = 0;
    std::string request_id;

    std::string Serialize() const;
    static Result<DeleteCmd> Deserialize(std::string_view data);
};

struct LogEntry {
    uint64_t index = 0;
    uint64_t term = 0;
    EntryType type = EntryType::kNormal;
    CmdType cmd_type = CmdType::kUpsert;
    // The command payload itself is serialized with protobuf so it can be
    // stored in WAL and replayed into the state machine later.
    std::string payload;

    std::string Serialize() const;
    static Result<LogEntry> Deserialize(std::string_view data);
};
