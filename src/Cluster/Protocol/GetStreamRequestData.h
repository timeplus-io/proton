#pragma once

#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/StreamDescriptor.h>

namespace cluster::protocol
{
struct GetStreamRequestData final : public ProtocolData
{
    /// Used for deserialization
    GetStreamRequestData() = default;

    GetStreamRequestData(
        std::string && ns_,
        std::string && stream_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_)
        : ns(std::move(ns_))
        , stream(std::move(stream_))
        , versions_requested(versions_requested_)
        , initiator(initiator_)
        , consistent_read(consistent_read_)
        , timeout_ms(timeout_ms_)
    {
    }

    GetStreamRequestData(
        const std::string & ns_,
        const std::string & stream_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_)
        : ns(ns_)
        , stream(stream_)
        , versions_requested(versions_requested_)
        , initiator(initiator_)
        , consistent_read(consistent_read_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::GetStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string ns;
    std::string stream;
    uint64_t versions_requested = 0;

    cluster::NodeID initiator = 0x0;
    bool consistent_read = false;
    int64_t timeout_ms = 0;
};

using GetStreamRequestDataPtr = std::shared_ptr<GetStreamRequestData>;
}
