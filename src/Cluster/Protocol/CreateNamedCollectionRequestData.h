#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>


namespace cluster::protocol
{
struct CreateNamedCollectionRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateNamedCollectionRequestData() = default;

    CreateNamedCollectionRequestData(
        std::string collection_name_,
        NamedCollectionDescriptorPtr collection_,
        ExistsOperation exists_op_,
        std::string requested_by_,
        NodeID initiator_,
        int64_t timeout_ms_)
        : collection_name(std::move(collection_name_))
        , collection(std::move(collection_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , exists_op(exists_op_)
        , requested_by(std::move(requested_by_))
        , requested_ts(DB::UTCMilliseconds::now())

    {
    }

    [[nodiscard]] OpCode opCode() const noexcept override { return OpCode::CreateNamedCollection; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override
    {
        return collection->approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms) + sizeof(exists_op)
            + approximateSerializedSizeOf(collection_name, requested_by) + sizeof(requested_ts);
    }

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string collection_name;
    NamedCollectionDescriptorPtr collection;

    NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms;

    ExistsOperation exists_op;
    std::string requested_by;
    int64_t requested_ts{0};
};

using CreateNamedCollectionRequestDataPtr = std::shared_ptr<CreateNamedCollectionRequestData>;
}
