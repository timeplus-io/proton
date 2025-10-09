#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/DiskDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>


namespace cluster::protocol
{
struct CreateDiskRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateDiskRequestData() = default;

    explicit CreateDiskRequestData(DiskDescriptorPtr disk_desc, cluster::NodeID initiator_, int64_t timeout_ms_)
        : disk(std::move(disk_desc)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateDisk; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    DiskDescriptorPtr disk;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using CreateDiskRequestDataPtr = std::shared_ptr<CreateDiskRequestData>;
}
