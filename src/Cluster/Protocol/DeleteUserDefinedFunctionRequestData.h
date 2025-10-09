#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteUserDefinedFunctionRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteUserDefinedFunctionRequestData() = default;

    DeleteUserDefinedFunctionRequestData(std::string && func_name_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : func_name(std::move(func_name_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    DeleteUserDefinedFunctionRequestData(const std::string & func_name_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : func_name(func_name_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteUserDefinedFunction; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string func_name;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DeleteUserDefinedFunctionRequestDataPtr = std::shared_ptr<DeleteUserDefinedFunctionRequestData>;
}
