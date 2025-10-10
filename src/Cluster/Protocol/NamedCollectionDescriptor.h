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
enum class NamedCollectionOverridability : uint8_t
{
    Default = 0,
    Overridable = 1,
    NotOverridable = 2,
};

struct NamedCollectionEntry
{
    std::string key;
    std::string value;
    NamedCollectionOverridability overridability = NamedCollectionOverridability::Default;
};
using NamedCollectionEntrys = std::vector<NamedCollectionEntry>;

SERDE struct NamedCollectionDescriptor
{
public:
    NamedCollectionDescriptor() = default;

    explicit NamedCollectionDescriptor(NamedCollectionEntrys entries_) : entries(std::move(entries_)) { }

    NamedCollectionDescriptor(NamedCollectionEntrys entries_, uint32_t data_version_)
        : data_version(data_version_), entries(std::move(entries_))
    {
    }

    NamedCollectionDescriptor(const NamedCollectionDescriptor &) = default;
    NamedCollectionDescriptor(NamedCollectionDescriptor &&) = default;

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    [[nodiscard]] size_t approximateSerializedSize() const noexcept;

    [[nodiscard]] std::string string() const;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    NamedCollectionEntrys entries;

    int64_t create_timestamp_ms{0};
    int64_t last_modify_timestamp_ms{0};

    std::string created_by;
    std::string last_modified_by;
};

using NamedCollectionDescriptorPtr = std::shared_ptr<NamedCollectionDescriptor>;
using NamedCollectionDescriptorPtrs = std::vector<NamedCollectionDescriptorPtr>;
using NamedCollectionDescriptors = std::vector<NamedCollectionDescriptor>;
}
