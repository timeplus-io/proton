#pragma once

#include <Cluster/Protocol/ResponseHeaderData.h>
#include <Cluster/Requests/RequestResponse.h>

namespace cluster
{
struct RequestHeader;

/// All internal responses are in format of: length + response header + response
struct ResponseHeader final : public RequestResponse
{
public:
    ResponseHeader() : RequestResponse(Nulls::NullVersion) { }

    ResponseHeader(protocol::OpCode opcode, uint16_t response_version, uint64_t correlation_id)
        : RequestResponse(Nulls::NullVersion), header_data(opcode, response_version, correlation_id)
    {
    }

    ResponseHeader(const ResponseHeader & other) : RequestResponse(other.data_version) { header_data = other.header_data; }

    ResponseHeader & operator=(const ResponseHeader & other)
    {
        data_version = other.data_version;
        header_data = other.header_data;

        return *this;
    }

    protocol::ResponseHeaderData & data() override { return header_data; }

    const protocol::ResponseHeaderData & data() const override { return header_data; }

    uint16_t responseVersion() const noexcept { return header_data.responseVersion(); }
    protocol::OpCode responseOpCode() const noexcept { return header_data.responseOpCode(); }
    std::string responseOpCodeString() const;

    uint16_t headerVersion() const noexcept { return header_data.headerVersion(); }

    uint64_t correlationID() const noexcept { return header_data.correlationID(); }

    std::string string() const override;

    int64_t elapsedMillisecondsSinceCreation() const noexcept { return header_data.elapsedMillisecondsSinceCreation(); }

    bool operator==(const ResponseHeader & other) const noexcept { return header_data == other.header_data; }

    /// Calculate response header automatically according to request header
    static ResponseHeader from(const RequestHeader & header, protocol::OpCode opcode_overridden = protocol::OpCode::Null);

    static ResponseHeader parse(DB::ReadBuffer & rb);

private:
    protocol::ResponseHeaderData header_data;
};
}
