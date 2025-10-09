#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct RenameStreamRequestData final : public ProtocolData
{
    /// Used for deserialization
    RenameStreamRequestData() = default;

    RenameStreamRequestData(
        std::string && ns_,
        std::string && stream_,
        std::string && new_stream_,
        std::string && sql_ddl_,
        std::string && modified_by_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : sql_ddl(std::move(sql_ddl_))
        , ns(std::move(ns_))
        , stream(std::move(stream_))
        , new_stream(std::move(new_stream_))
        , modified_by(std::move(modified_by_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::RenameStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    /// DDL SQL which triggers the C/U/D mutate request
    std::string sql_ddl;

    std::string ns;
    std::string stream;
    std::string new_stream;

    /// Added by v2 schema
    std::string modified_by;

    cluster::NodeID initiator = 0x0;
    int64_t timeout_ms = 0;
};

using RenameStreamRequestDataPtr = std::shared_ptr<RenameStreamRequestData>;
}
