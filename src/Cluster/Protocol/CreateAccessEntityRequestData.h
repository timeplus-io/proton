#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <base/UUID.h>

namespace cluster::protocol
{
struct CreateAccessEntityRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateAccessEntityRequestData() = default;

    CreateAccessEntityRequestData(AccessEntityDescription desc_, bool replace_if_exists_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : desc(std::move(desc_)), replace_if_exists(replace_if_exists_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateAccessEntity; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    AccessEntityDescription desc;
    bool replace_if_exists;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using CreateAccessEntityRequestDataPtr = std::shared_ptr<CreateAccessEntityRequestData>;
}
