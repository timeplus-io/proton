#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <utility>


namespace cluster::protocol
{
struct DeleteNamedCollectionRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteNamedCollectionRequestData() = default;

    DeleteNamedCollectionRequestData(std::string collection_name_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : collection_name(std::move(collection_name_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteNamedCollection; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override;

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string collection_name;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DeleteNamedCollectionRequestDataPtr = std::shared_ptr<DeleteNamedCollectionRequestData>;
}
