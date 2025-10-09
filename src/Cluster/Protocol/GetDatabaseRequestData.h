#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct GetDatabaseRequestData final : public ProtocolData
{
    GetDatabaseRequestData() = default;

    GetDatabaseRequestData(
        std::string && name_, uint64_t versions_requested_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : name(std::move(name_))
        , versions_requested(versions_requested_)
        , initiator(initiator_)
        , consistent_read(consistent_read_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::GetDatabase; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    std::string name;
    uint64_t versions_requested = 0;

    cluster::NodeID initiator = Nulls::NullNodeID;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using GetDatabaseRequestDataPtr = std::shared_ptr<GetDatabaseRequestData>;
}
