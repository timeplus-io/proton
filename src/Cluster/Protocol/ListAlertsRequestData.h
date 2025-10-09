#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct ListAlertsRequestData final : public ProtocolData
{
    /// Used for deserialization
    ListAlertsRequestData() = default;

    ListAlertsRequestData(std::string && ns_, std::string && name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : ns(std::move(ns_)), name(std::move(name_)), initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    ListAlertsRequestData(
        const std::string & ns_, const std::string & name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : ns(ns_), name(name_), initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ListAlerts; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    std::string ns;
    std::string name;

    cluster::NodeID initiator = Nulls::NullNodeID;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using ListAlertsRequestDataPtr = std::shared_ptr<ListAlertsRequestData>;
}
