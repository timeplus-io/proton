#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>


namespace cluster::protocol
{
struct ListNamedCollectionsRequestData final : public ProtocolData
{
    /// Used for deserialization
    ListNamedCollectionsRequestData() = default;

    ListNamedCollectionsRequestData(cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ListNamedCollections; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    [[nodiscard]] std::string doString() const override;

public:
    cluster::NodeID initiator = Nulls::NullNodeID;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using ListNamedCollectionsRequestDataPtr = std::shared_ptr<ListNamedCollectionsRequestData>;
}
