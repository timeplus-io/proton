#include <Cluster/Common/Stream.h>
#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void Stream::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);
    DB::writeBinary(id, wb);
}

void Stream::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);
    DB::readBinary(id, rb);
}

size_t Stream::approximateSerializedSize() const noexcept
{
    return sizeof(Stream) + approximateSerializedSizeOf(ns, name);
}

std::string Stream::string() const
{
    return fmt::format("ns={} name={} id={}", ns, name, DB::toString(id));
}

}
