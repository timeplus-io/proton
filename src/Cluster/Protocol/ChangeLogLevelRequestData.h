#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct ChangeLogLevelRequestData final : public ProtocolData
{
    /// Used for deserialization
    ChangeLogLevelRequestData() = default;

    ChangeLogLevelRequestData(std::string && log_level_, std::string && logger_name_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : log_level(std::move(log_level_)), logger_name(std::move(logger_name_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    ~ChangeLogLevelRequestData() override;

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ChangeLogLevel; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string log_level;
    std::string logger_name;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using ChangeLogLevelRequestDataPtr = std::shared_ptr<ChangeLogLevelRequestData>;
}
