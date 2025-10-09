#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>

namespace cluster::protocol
{
struct CreateFormatSchemaRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateFormatSchemaRequestData() = default;

    CreateFormatSchemaRequestData(
        FormatSchemaDescriptor && desc_,
        ExistsOperation exists_op_,
        const String & requested_by_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : desc(std::move(desc_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , exists_op(exists_op_)
        , requested_by(requested_by_)
        , requested_ts(DB::UTCMilliseconds::now())
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateFormatSchema; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 3}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override { return desc.approximateSerializedSize(); }

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    FormatSchemaDescriptor desc;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;

    /// Added by v3 schema
    ExistsOperation exists_op;
    std::string requested_by;
    int64_t requested_ts{0};
};

using CreateFormatSchemaRequestDataPtr = std::shared_ptr<CreateFormatSchemaRequestData>;
}
