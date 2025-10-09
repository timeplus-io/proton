#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Cluster/Protocol/ExistsOperation.h>

namespace cluster::protocol
{
struct CreateUserDefinedFunctionRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateUserDefinedFunctionRequestData() = default;

    CreateUserDefinedFunctionRequestData(
        UserDefinedFunctionDescriptor && desc_, ExistsOperation exists_op_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : desc(std::move(desc_)), initiator(initiator_), timeout_ms(timeout_ms_), exists_op(exists_op_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateUserDefinedFunction; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 3}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    UserDefinedFunctionDescriptor desc;

    /// Added in v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;

    /// Added in v3 schema
    ExistsOperation exists_op;
};

using CreateUserDefinedFunctionRequestDataPtr = std::shared_ptr<CreateUserDefinedFunctionRequestData>;
}
