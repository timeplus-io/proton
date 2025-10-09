#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/TaskDescriptor.h>

#include <base/ClockUtils.h>

#include <utility>


namespace cluster::protocol
{
struct CreateTaskRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateTaskRequestData() = default;

    CreateTaskRequestData(
        TaskDescriptor && desc_,
        cluster::protocol::ExistsOperation exists_op_,
        std::string requested_by_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : desc(std::move(desc_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , exists_op(exists_op_)
        , requested_by(std::move(requested_by_))
        , requested_ts(DB::UTCMilliseconds::now())

    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateTask; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override
    {
        return desc.approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms) + sizeof(exists_op)
            + approximateSerializedSizeOf(requested_by) + sizeof(requested_ts);
    }

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    TaskDescriptor desc;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms;

    ExistsOperation exists_op;
    std::string requested_by;
    int64_t requested_ts{0};
};

using CreateTaskRequestDataPtr = std::shared_ptr<CreateTaskRequestData>;
}
