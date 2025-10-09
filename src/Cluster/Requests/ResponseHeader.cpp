#include <Cluster/Requests/RequestHeader.h>
#include <Cluster/Requests/ResponseHeader.h>

#include <fmt/format.h>

namespace cluster
{
std::string ResponseHeader::string() const
{
    return fmt::format("header_version={} {}", headerVersion(), header_data.string());
}

std::string ResponseHeader::responseOpCodeString() const
{
    return fmt::format("{}", magic_enum::enum_name(responseOpCode()));
}

ResponseHeader ResponseHeader::from(const RequestHeader & header, protocol::OpCode opcode_overridden)
{
    auto response_opcode = (opcode_overridden == protocol::OpCode::Null ? header.requestOpCode() : opcode_overridden);
    /// FIXME, figure out the response version and header version automatically
    auto response_version = header.requestVersion();
    /// auto response_header_version = responseHeaderVersion(request_opcode, request_version);
    return ResponseHeader(response_opcode, response_version, header.correlationID());
}

ResponseHeader ResponseHeader::parse(DB::ReadBuffer & rb)
{
    ResponseHeader header;
    header.header_data.deserialize(rb, Nulls::NullVersion);
    return header;
}

}
