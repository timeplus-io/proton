#include <Cluster/Protocol/DropFormatSchemaCacheRequestData.h>

#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace cluster::protocol
{

void DropFormatSchemaCacheRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(caches_to_drop.size(), wb);
    for (const auto & format : caches_to_drop)
        DB::writeStringBinary(format, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DropFormatSchemaCacheRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    size_t format_count{0};
    DB::readVarUInt(format_count, rb);

    for (size_t i = 0; i < format_count; ++i)
    {
        std::string format;
        DB::readStringBinary(format, rb);
        caches_to_drop.emplace(std::move(format));
    }

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DropFormatSchemaCacheRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(caches_to_drop) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DropFormatSchemaCacheRequestData::doString() const
{
    return fmt::format("caches_to_drop={} initiator=0x{:x} timeout_ms={}", fmt::join(caches_to_drop, ","), initiator, timeout_ms);
}

}
