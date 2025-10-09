#pragma once

#include <Cluster/Common/Nulls.h>
#include <base/UUID.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{

struct AccessEntityDescription
{
public:
    AccessEntityDescription() = default;

    AccessEntityDescription(const AccessEntityDescription &) = default;
    AccessEntityDescription & operator=(const AccessEntityDescription &) = default;
    AccessEntityDescription(AccessEntityDescription && other) noexcept = default;
    AccessEntityDescription & operator=(AccessEntityDescription && other) noexcept = default;

    AccessEntityDescription(const DB::UUID & id_, uint32_t data_version_, std::string entity_)
        : id(id_), data_version(data_version_), entity(std::move(entity_))
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    size_t approximateSerializedSize() const noexcept;

    std::string string() const;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;

    DB::UUID id;

    /// `version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;
    std::string entity;
};

using AccessEntityDescriptionPtr = std::shared_ptr<AccessEntityDescription>;
using AccessEntityDescriptionPtrs = std::vector<AccessEntityDescriptionPtr>;
using AccessEntityDescriptions = std::vector<AccessEntityDescription>;
}
