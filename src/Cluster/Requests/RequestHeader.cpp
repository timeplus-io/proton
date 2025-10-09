#include <Cluster/Requests/RequestHeader.h>

#include <fmt/format.h>

namespace cluster
{
std::string RequestHeader::string() const
{
    return fmt::format("header_version={} {}", headerVersion(), header_data.string());
}

RequestHeader RequestHeader::parse(DB::ReadBuffer & rb)
{
    RequestHeader header;
    header.header_data.deserialize(rb, Nulls::NullVersion);
    return header;
}

std::string RequestHeader::requestOpCodeString() const
{
    return fmt::format("{}", magic_enum::enum_name(requestOpCode()));
}
}
