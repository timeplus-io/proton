#pragma once

#include <base/UUID.h>


namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
struct DatabaseDescriptor
{
public:
    DatabaseDescriptor() = default;

    DatabaseDescriptor(std::string name_, std::string sql_def_) : name(std::move(name_)), sql_def(std::move(sql_def_)) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    size_t approximateSerializedSize() const noexcept;

    std::string string() const;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;

    std::string name;
    std::string sql_def;
    std::string comment;
    DB::UUID uuid;

    int64_t create_timestamp_ms;
    int64_t last_modify_timestamp_ms;
    std::string created_by;
    std::string last_modified_by;
};

using DatabaseDescriptorPtr = std::shared_ptr<DatabaseDescriptor>;
using DatabaseDescriptorPtrs = std::vector<DatabaseDescriptorPtr>;
using DatabaseDescriptors = std::vector<DatabaseDescriptor>;
}
