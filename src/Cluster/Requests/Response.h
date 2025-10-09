#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Requests/RequestResponse.h>

namespace cluster
{

struct Response;
struct ResponseHeader;
struct RequestHeader;
struct Error;

using ResponsePtr = std::shared_ptr<Response>;

struct Response : public RequestResponse
{
    using RequestResponse::RequestResponse;

    /// Serializes response header and body without prefixing with size
    /// Calculate response header automatically according to request header
    void serializeWithHeader(DB::WriteBuffer & wb, const ResponseHeader & header) const;

    /// We like to use ByteVector here since we like to detach the allocated memory
    /// to pass it to C language.
    ByteVector serializeWithHeader(const ResponseHeader & header) const;
    std::string serializeWithHeaderInString(const ResponseHeader & header) const;

    /// Serializes response header and body with prefixing with size
    ByteVector serializeWithSizeAndHeader(const ResponseHeader & header) const;
    ByteVector serializeWithSizeAndHeader(const RequestHeader & header) const;

    bool hasPermanentError() const noexcept;

    virtual bool hasError() const noexcept = 0;
    virtual const Error & error() const noexcept = 0;
    virtual Error & error() noexcept = 0;

    static ResponsePtr parse(protocol::OpCode opcode, uint16_t version, DB::ReadBuffer & rb);

    static ResponsePtr parse(protocol::OpCode opcode, uint16_t version, std::string_view data);

    static ResponsePtr parseWithHeader(std::string_view data, ResponseHeader & header);
};
}
