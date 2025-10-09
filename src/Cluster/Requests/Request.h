#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Requests/RequestHeader.h>
#include <Cluster/Requests/RequestResponse.h>

namespace cluster
{

class Request;

using RequestPtr = std::shared_ptr<Request>;

using RequestPtrs = std::vector<RequestPtr>;

struct RequestAndSize
{
    RequestAndSize() = default;

    RequestAndSize(RequestPtr request_, uint32_t size_) : request(std::move(request_)), size(size_) { }

    RequestPtr request;
    uint32_t size = 0;
};

class Request : public RequestResponse
{
public:
    using RequestResponse::RequestResponse;

    /// Serializes request header and body without prefixing with size
    void serializeWithHeader(DB::WriteBuffer & wb, const RequestHeader & header) const;

    /// We like to use ByteVector here since we like to detach the allocated memory
    /// to pass it to C language.
    ByteVector serializeWithHeader(const RequestHeader & header) const;

    /// Serializes request header and body with prefixing with size
    ByteVector serializeWithSizeAndHeader(const RequestHeader & header) const;

    /// @param data includes RequestHeader, parse them together
    static RequestAndSize parseWithHeader(std::string_view data, RequestHeader & header);

    static RequestPtr parse(protocol::OpCode request_opcode, uint16_t request_version, DB::ReadBuffer & rb);

    static RequestPtr parse(protocol::OpCode request_opcode, uint16_t request_version, std::string_view data);
};
}
