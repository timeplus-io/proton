#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>

namespace cluster::protocol
{
/// All internal requests on the wire are prefixed with length, then RequestHeaderData and then
/// followed by payload.
class RequestHeaderData final : public ProtocolData
{
public:
    /// using ProtocolData::serialize;

    RequestHeaderData() = default;

    RequestHeaderData(protocol::OpCode request_opcode_, uint16_t request_version_, uint64_t correlation_id_)
        : request_opcode(request_opcode_), request_version(request_version_), correlation_id(correlation_id_)
    {
    }

    RequestHeaderData(protocol::OpCode request_opcode_, uint16_t request_version_, uint64_t correlation_id_, int64_t timestamp_ms_)
        : request_opcode(request_opcode_)
        , request_version(request_version_)
        , correlation_id(correlation_id_)
        , timestamp_ms(timestamp_ms_)
    {
    }

    RequestHeaderData(const RequestHeaderData & other)
    {
        request_opcode = other.request_opcode;
        request_version = other.request_version;
        correlation_id = other.correlation_id;
        timestamp_ms = other.timestamp_ms;
    }

    RequestHeaderData & operator=(const RequestHeaderData & other)
    {
        request_opcode = other.request_opcode;
        request_version = other.request_version;
        correlation_id = other.correlation_id;
        timestamp_ms = other.timestamp_ms;

        return *this;
    }

    /// Let's pay attention here, the op code of RequestHeaderData is Null
    /// `requestOpCode` represent its payload opcode
    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::Null; }

    protocol::OpCode requestOpCode() const noexcept { return request_opcode; }
    uint16_t requestVersion() const noexcept { return request_version; }
    uint64_t correlationID() const noexcept { return correlation_id; }
    uint16_t headerVersion() const { return requestHeaderVersion(request_opcode, request_version); }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    /// Marshal the request to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override { return sizeof(RequestHeaderData); }

    /// string representation for logging / debug purpose
    std::string doString() const override;

    int64_t elapsedMillisecondsSinceCreation() const noexcept { return DB::UTCMilliseconds::now() - timestamp_ms; }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

private:
    /// PrefixLength + RequestHeader + Payload
    /// This is the version of the payload
    /// RequestHeader itself has its version which can be deduced via request opcode + request version
    /// We can't change the serialization order of request_opcode / request_version since we will
    /// need bootstrap / calculate the version of itself (RequestHeader) by using them
    protocol::OpCode request_opcode = OpCode::Null;
    uint16_t request_version = Nulls::NullVersion;
    uint64_t correlation_id = 0;
    /// When this header / request is created, UTC milliseconds
    int64_t timestamp_ms = DB::UTCMicroseconds::now();
};
}
