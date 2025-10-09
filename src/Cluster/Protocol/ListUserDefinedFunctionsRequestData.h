#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

namespace cluster::protocol
{
struct ListUserDefinedFunctionsRequestData final : public ProtocolData
{
    /// Used for deserialization
    ListUserDefinedFunctionsRequestData() = default;

    ListUserDefinedFunctionsRequestData(std::string && func_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : func_name(std::move(func_name_)), initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    ListUserDefinedFunctionsRequestData(
        const std::string & func_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : func_name(func_name_), initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ListUserDefinedFunctions; }

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
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using ListUserDefinedFunctionsRequestDataPtr = std::shared_ptr<ListUserDefinedFunctionsRequestData>;
}
