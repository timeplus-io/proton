#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

namespace cluster::protocol
{
struct GetUserDefinedFunctionRequestData final : public ProtocolData
{
    /// Used for deserialization
    GetUserDefinedFunctionRequestData() = default;

    GetUserDefinedFunctionRequestData(
        std::string && name_, uint64_t versions_requested_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : name(std::move(name_))
        , versions_requested(versions_requested_)
        , initiator(initiator_)
        , consistent_read(consistent_read_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::GetUserDefinedFunction; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string name;
    uint64_t versions_requested = 0;

    cluster::NodeID initiator = Nulls::NullNodeID;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using GetUserDefinedFunctionRequestDataPtr = std::shared_ptr<GetUserDefinedFunctionRequestData>;
}
