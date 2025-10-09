#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>

namespace cluster::protocol
{
struct UpdateStreamSchemaRequestData final : public ProtocolData
{
    /// Used for deserialization
    UpdateStreamSchemaRequestData() = default;

    UpdateStreamSchemaRequestData(
        std::string && database_,
        std::string && table_,
        StreamID id_,
        std::vector<std::string> && alter_commands_,
        std::string && sql_def_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint32_t version_before_update_)
        : version_before_update(version_before_update_)
        , stream(std::move(database_), std::move(table_), id_)
        , alter_commands(std::move(alter_commands_))
        , sql_def(std::move(sql_def_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::AlterStreamSchema; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 3}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    uint32_t version_before_update{Nulls::NullVersion};
    Stream stream;
    std::vector<std::string> alter_commands;

    /// New stream def
    std::string sql_def;

    /// Added in v2 schema
    std::string modified_by;
    int64_t last_modified = DB::UTCMilliseconds::now();

    cluster::NodeID initiator = 0x0;
    int64_t timeout_ms = 0;
};

using UpdateStreamSchemaRequestDataPtr = std::shared_ptr<UpdateStreamSchemaRequestData>;
}
