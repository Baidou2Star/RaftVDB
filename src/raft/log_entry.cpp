#include "raft/log_entry.hpp"

#include <type_traits>

#include "raft.pb.h"

namespace {

namespace proto = raftvdb::proto;

template <typename EnumType>
bool IsKnownEnumValue(uint32_t value);

template <>
bool IsKnownEnumValue<EntryType>(uint32_t value) {
    switch (static_cast<EntryType>(value)) {
    case EntryType::kNormal:
    case EntryType::kConfig:
    case EntryType::kNoop:
        return true;
    }
    return false;
}

template <>
bool IsKnownEnumValue<CmdType>(uint32_t value) {
    switch (static_cast<CmdType>(value)) {
    case CmdType::kUpsert:
    case CmdType::kDelete:
        return true;
    }
    return false;
}

template <typename ProtoType>
Result<std::string> SerializeProto(const ProtoType& message, std::string_view type_name) {
    std::string buffer;
    if (!message.SerializeToString(&buffer)) {
        return Result<std::string>::Err("protobuf 序列化失败: " + std::string(type_name));
    }
    return Result<std::string>::Ok(std::move(buffer));
}

template <typename ProtoType>
Result<ProtoType> ParseProto(std::string_view data, std::string_view type_name) {
    ProtoType message;
    if (!message.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
        return Result<ProtoType>::Err("protobuf 反序列化失败: " + std::string(type_name));
    }
    return Result<ProtoType>::Ok(std::move(message));
}

} // namespace

std::string UpsertCmd::Serialize() const {
    proto::UpsertCmd message;
    message.set_id(id);
    for (float value : vector) {
        message.add_vector(value);
    }
    message.set_request_id(request_id);

    auto serialized = SerializeProto(message, "UpsertCmd");
    return serialized ? std::move(*serialized) : std::string{};
}

Result<UpsertCmd> UpsertCmd::Deserialize(std::string_view data) {
    auto parsed = ParseProto<proto::UpsertCmd>(data, "UpsertCmd");
    if (!parsed) {
        return Result<UpsertCmd>::Err(parsed.error);
    }

    UpsertCmd command;
    command.id = parsed->id();
    command.request_id = parsed->request_id();
    command.vector.assign(parsed->vector().begin(), parsed->vector().end());
    return Result<UpsertCmd>::Ok(std::move(command));
}

std::string DeleteCmd::Serialize() const {
    proto::DeleteCmd message;
    message.set_id(id);
    message.set_request_id(request_id);

    auto serialized = SerializeProto(message, "DeleteCmd");
    return serialized ? std::move(*serialized) : std::string{};
}

Result<DeleteCmd> DeleteCmd::Deserialize(std::string_view data) {
    auto parsed = ParseProto<proto::DeleteCmd>(data, "DeleteCmd");
    if (!parsed) {
        return Result<DeleteCmd>::Err(parsed.error);
    }

    DeleteCmd command;
    command.id = parsed->id();
    command.request_id = parsed->request_id();
    return Result<DeleteCmd>::Ok(std::move(command));
}

std::string LogEntry::Serialize() const {
    proto::LogEntry message;
    message.set_index(index);
    message.set_term(term);
    message.set_type(static_cast<uint32_t>(type));
    message.set_cmd_type(static_cast<uint32_t>(cmd_type));
    message.set_payload(payload);

    auto serialized = SerializeProto(message, "LogEntry");
    return serialized ? std::move(*serialized) : std::string{};
}

Result<LogEntry> LogEntry::Deserialize(std::string_view data) {
    auto parsed = ParseProto<proto::LogEntry>(data, "LogEntry");
    if (!parsed) {
        return Result<LogEntry>::Err(parsed.error);
    }
    if (!IsKnownEnumValue<EntryType>(parsed->type())) {
        return Result<LogEntry>::Err("LogEntry.type 非法");
    }
    if (!IsKnownEnumValue<CmdType>(parsed->cmd_type())) {
        return Result<LogEntry>::Err("LogEntry.cmd_type 非法");
    }

    LogEntry entry;
    entry.index = parsed->index();
    entry.term = parsed->term();
    entry.type = static_cast<EntryType>(parsed->type());
    entry.cmd_type = static_cast<CmdType>(parsed->cmd_type());
    entry.payload = parsed->payload();
    return Result<LogEntry>::Ok(std::move(entry));
}
