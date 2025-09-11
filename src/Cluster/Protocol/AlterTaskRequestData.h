#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/TaskDescriptor.h>

#include <base/ClockUtils.h>

#include <utility>


namespace cluster::protocol
{
struct AlterTaskRequestData final : public ProtocolData
{
    enum class AlterType : uint8_t
    {
        STATUS,
    };

    /// Used for deserialization
    AlterTaskRequestData() = default;

    /// Update task status
    AlterTaskRequestData(
        std::string ns_,
        std::string name_,
        DB::UUID id_,
        uint32_t data_version_,
        AlterType type_,
        protocol::TaskStatus status_,
        std::string requested_by_,
        NodeID initiator_,
        int64_t timeout_ms_)
        : ns(std::move(ns_))
        , name(std::move(name_))
        , id(std::move(id_))
        , data_version(data_version_)
        , type(type_)
        , status(status_)
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , requested_by(std::move(requested_by_))
        , requested_ts(DB::UTCMilliseconds::now())
    {
    }

    [[nodiscard]] OpCode opCode() const noexcept override { return OpCode::AlterTask; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override
    {
        return sizeof(id) + sizeof(data_version) + sizeof(type) + sizeof(status) + sizeof(initiator) + sizeof(timeout_ms)
            + approximateSerializedSizeOf(ns, name, requested_by) + sizeof(requested_ts);
    }

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string ns;
    std::string name;
    DB::UUID id;
    uint32_t data_version;

    AlterType type;
    TaskStatus status;

    NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms;

    std::string requested_by;
    int64_t requested_ts{0};
};

using UpdateTaskRequestDataPtr = std::shared_ptr<AlterTaskRequestData>;
}
