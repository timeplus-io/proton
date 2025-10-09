#pragma once

#include <Cluster/Common/Common.h>
#include <Cluster/Common/SerdeTag.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
SERDE struct FormatSchemaDescriptor
{
public:
    FormatSchemaDescriptor() = default;

    FormatSchemaDescriptor(const FormatSchemaDescriptor &) = default;
    FormatSchemaDescriptor(FormatSchemaDescriptor &&) = default;

    FormatSchemaDescriptor(
        const std::string & name_,
        const std::string & format_,
        const std::string & schema_body_,
        uint32_t data_version_)
        : data_version(data_version_), name(name_), format(format_), schema_body(schema_body_)
    {
    }

    FormatSchemaDescriptor(
        std::string && name_, std::string && format_, std::string && schema_body_, uint32_t data_version_)
        : data_version(data_version_)
        , name(std::move(name_))
        , format(std::move(format_))
        , schema_body(std::move(schema_body_))
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    size_t approximateSerializedSize() const noexcept;

    bool operator==(const FormatSchemaDescriptor & rhs) const
    {
        return data_version == rhs.data_version && name == rhs.name && format == rhs.format && schema_body == rhs.schema_body;
    }

    std::string string() const;

    bool isValid() const noexcept { return data_version > 0 && !name.empty() && !format.empty() && !schema_body.empty(); }

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 2;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    std::string name;
    std::string format;
    std::string schema_body;

    /// Added in version 2
    int64_t create_timestamp_ms{0};
    int64_t last_modify_timestamp_ms{0};

    std::string created_by;
    std::string last_modified_by;
};

using FormatSchemaDescriptorPtr = std::shared_ptr<FormatSchemaDescriptor>;
using FormatSchemaDescriptorPtrs = std::vector<FormatSchemaDescriptorPtr>;
using FormatSchemaDescriptors = std::vector<FormatSchemaDescriptor>;
}
