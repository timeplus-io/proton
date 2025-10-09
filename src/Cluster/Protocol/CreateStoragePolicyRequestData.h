#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>

#include <base/defines.h>

namespace cluster::protocol
{
struct CreateStoragePolicyRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateStoragePolicyRequestData() = default;

    explicit CreateStoragePolicyRequestData(StoragePolicyDescriptorPtr policy_desc, cluster::NodeID initiator_, int64_t timeout_ms_)
        : policy(std::move(policy_desc)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateStoragePolicy; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    StoragePolicyDescriptorPtr policy;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using CreateStoragePolicyRequestDataPtr = std::shared_ptr<CreateStoragePolicyRequestData>;
}
