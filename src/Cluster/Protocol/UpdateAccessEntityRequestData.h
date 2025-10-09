#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct UpdateAccessEntityRequestData final : public ProtocolData
{
    /// Used for deserialization
    UpdateAccessEntityRequestData() = default;

    UpdateAccessEntityRequestData(
        uint32_t version_before_update_, AccessEntityDescription new_entity_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : version_before_update(version_before_update_), new_entity(std::move(new_entity_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::AlterAccessEntity; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    uint32_t version_before_update = 0x0;
    AccessEntityDescription new_entity;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using UpdateAccessEntityRequestDataPtr = std::shared_ptr<UpdateAccessEntityRequestData>;
}
