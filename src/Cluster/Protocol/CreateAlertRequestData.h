#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>

#include <utility>

namespace cluster::protocol
{
struct CreateAlertRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateAlertRequestData() = default;

    CreateAlertRequestData(
        AlertDescriptor && desc_, ExistsOperation exists_op_, String requested_by_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : desc(std::move(desc_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , exists_op(exists_op_)
        , requested_by(std::move(requested_by_))
        , requested_ts(DB::UTCMilliseconds::now())
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateAlert; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override { return desc.approximateSerializedSize(); }

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    AlertDescriptor desc;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;

    ExistsOperation exists_op;
    std::string requested_by;
    int64_t requested_ts{0};
};

using CreateAlertRequestDataPtr = std::shared_ptr<CreateAlertRequestData>;
}
