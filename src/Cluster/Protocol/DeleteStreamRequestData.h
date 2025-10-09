#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteStreamRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteStreamRequestData() = default;

    DeleteStreamRequestData(
        std::string && ns_,
        std::string && stream_,
        const StreamID & stream_id_,
        std::string && sql_ddl_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : sql_ddl(std::move(sql_ddl_))
        , stream(std::move(ns_), std::move(stream_), stream_id_)
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    DeleteStreamRequestData(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & stream_id_,
        std::string && sql_ddl_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : sql_ddl(sql_ddl_), stream(ns_, stream_, stream_id_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override
    {
        return approximateSerializedSizeOf(sql_ddl) + stream.approximateSerializedSize();
    }

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    /// DDL SQL which triggers the C/U/D mutate request
    std::string sql_ddl;

    Stream stream;

    /// Added by v2 schema
    cluster::NodeID initiator = 0x0;
    int64_t timeout_ms = 0;
};

using DeleteStreamRequestDataPtr = std::shared_ptr<DeleteStreamRequestData>;
}
