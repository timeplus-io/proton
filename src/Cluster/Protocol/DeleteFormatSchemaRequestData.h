#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteFormatSchemaRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteFormatSchemaRequestData() = default;

    DeleteFormatSchemaRequestData(
        std::string && schema_name_, std::string && format_, bool if_exists_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : schema_name(std::move(schema_name_)), format(format_), if_exists(if_exists_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    DeleteFormatSchemaRequestData(
        const std::string & schema_name_, const std::string & format_, bool if_exists_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : schema_name(schema_name_), format(format_), if_exists(if_exists_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteFormatSchema; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string schema_name;
    std::string format;
    bool if_exists;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DeleteFormatSchemaRequestDataPtr = std::shared_ptr<DeleteFormatSchemaRequestData>;
}
