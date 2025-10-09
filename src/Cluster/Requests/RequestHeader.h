#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/RequestHeaderData.h>
#include <Cluster/Requests/RequestResponse.h>

namespace cluster
{
/// All internal requests are in format of: length + request header + request
struct RequestHeader final : public RequestResponse
{
public:
    RequestHeader() : RequestResponse(Nulls::NullVersion) { }

    RequestHeader(protocol::OpCode request_opcode, uint16_t request_version, uint64_t correlation_id)
        : RequestResponse(Nulls::NullVersion), header_data(request_opcode, request_version, correlation_id)
    {
    }

    RequestHeader(const RequestHeader & other) : RequestResponse(other.data_version) { header_data = other.header_data; }

    RequestHeader & operator=(const RequestHeader & other)
    {
        data_version = other.data_version;
        header_data = other.header_data;

        return *this;
    }

    protocol::RequestHeaderData & data() override { return header_data; }

    const protocol::RequestHeaderData & data() const override { return header_data; }

    protocol::OpCode requestOpCode() const noexcept { return header_data.requestOpCode(); }

    std::string requestOpCodeString() const;

    uint16_t requestVersion() const noexcept { return header_data.requestVersion(); }

    uint16_t headerVersion() const noexcept { return header_data.headerVersion(); }

    uint64_t correlationID() const noexcept { return header_data.correlationID(); }

    std::string string() const override;

    int64_t elapsedMillisecondsSinceCreation() const noexcept { return header_data.elapsedMillisecondsSinceCreation(); }

    static RequestHeader parse(DB::ReadBuffer & rb);

private:
    protocol::RequestHeaderData header_data;
};
}
