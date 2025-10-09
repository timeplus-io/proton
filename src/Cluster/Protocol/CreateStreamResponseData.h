#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/StreamDescriptor.h>

namespace cluster::protocol
{
struct CreateStreamResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    CreateStreamResponseData() = default;

    CreateStreamResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit CreateStreamResponseData(Error && err_) : err(std::move(err_)) { }

    explicit CreateStreamResponseData(StreamDescriptor && stream_desc_, cluster::NodeID replica_leader_, int64_t sn_)
        : stream_desc(std::move(stream_desc_)), replica_leader(replica_leader_), sn(sn_)
    {
    }

    explicit CreateStreamResponseData(const StreamDescriptor & stream_desc_, cluster::NodeID replica_leader_, int64_t sn_)
        : stream_desc(stream_desc_), replica_leader(replica_leader_), sn(sn_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    /// Marshal the request / response to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    Error err;
    StreamDescriptor stream_desc;

    /// Added in v2 schema
    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using CreateStreamResponseDataPtr = std::shared_ptr<CreateStreamResponseData>;
}
