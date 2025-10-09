#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct ListFormatSchemasRequestData final : public ProtocolData
{
    /// Used for deserialization
    ListFormatSchemasRequestData() = default;

    ListFormatSchemasRequestData(
        std::string && schema_name_, std::string && format_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_)
        : schema_name(std::move(schema_name_))
        , format(std::move(format_))
        , initiator(initiator_)
        , consistent_read(consistent_read_)
        , timeout_ms(timeout_ms_)
    {
    }

    ListFormatSchemasRequestData(
        const std::string & schema_name_,
        const std::string & format_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_)
        : schema_name(schema_name_), format(format_), initiator(initiator_), consistent_read(consistent_read_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ListFormatSchemas; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    std::string schema_name;
    std::string format;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using ListFormatSchemasRequestDataPtr = std::shared_ptr<ListFormatSchemasRequestData>;
}
