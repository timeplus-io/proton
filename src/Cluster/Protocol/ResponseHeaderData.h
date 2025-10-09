#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <base/ClockUtils.h>

namespace cluster::protocol
{
/// All internal requests on the wire are prefixed with length, then ResponseHeaderData and then
/// followed by payload.
class ResponseHeaderData final : public ProtocolData
{
public:
    /// using ProtocolData::serialize;

    ResponseHeaderData() = default;

    ResponseHeaderData(protocol::OpCode response_opcode_, uint16_t response_version_, uint64_t correlation_id_)
        : response_opcode(response_opcode_), response_version(response_version_), correlation_id(correlation_id_)
    {
    }

    ResponseHeaderData(const ResponseHeaderData & other)
    {
        response_opcode = other.response_opcode;
        response_version = other.response_version;
        correlation_id = other.correlation_id;
        timestamp_ms = other.timestamp_ms;
    }

    ResponseHeaderData & operator=(const ResponseHeaderData & other)
    {
        response_opcode = other.response_opcode;
        response_version = other.response_version;
        correlation_id = other.correlation_id;
        timestamp_ms = other.timestamp_ms;

        return *this;
    }

    /// Let's pay attention here, the op code of ResponseHeaderData is Null
    /// `requestOpCode` represent its payload opcode
    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::Null; }

    protocol::OpCode responseOpCode() const noexcept { return response_opcode; }
    uint16_t responseVersion() const noexcept { return response_version; }
    uint64_t correlationID() const noexcept { return correlation_id; }
    uint16_t headerVersion() const { return requestHeaderVersion(response_opcode, response_version); }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    /// Marshal the request to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override { return sizeof(ResponseHeaderData); }

    /// string representation for logging / debug purpose
    std::string doString() const override;

    int64_t elapsedMillisecondsSinceCreation() const noexcept { return DB::UTCMilliseconds::now() - timestamp_ms; }

    bool operator==(const ResponseHeaderData & other) const noexcept
    {
        return response_opcode == other.response_opcode && response_version == other.response_version
            && correlation_id == other.correlation_id && timestamp_ms == other.timestamp_ms;
    }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

private:
    /// PrefixLength + ResponseHeader + Payload. So far ResponseHeader has the same schema
    /// as RequestHeader, but we still separate them since in future, they may diverge.
    /// This is the version of the payload
    /// ResponseHeader itself has its version which can be deduced via request opcode + request version
    /// We can't change the serialization order of response_opcode / response_version since we will
    /// need bootstrap / calculate the version of itself (ResponseHeader) by using them
    protocol::OpCode response_opcode = OpCode::Null;
    uint16_t response_version = Nulls::NullVersion;
    uint64_t correlation_id = 0;
    int64_t timestamp_ms = DB::UTCMicroseconds::now();
};
}
