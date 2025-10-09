#include <Cluster/Protocol/ChangeLogLevelRequestData.h>

#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{

ChangeLogLevelRequestData::~ChangeLogLevelRequestData() = default;

void ChangeLogLevelRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(log_level, wb);
    DB::writeStringBinary(logger_name, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ChangeLogLevelRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(log_level, rb);
    DB::readStringBinary(logger_name, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ChangeLogLevelRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(log_level) + approximateSerializedSizeOf(logger_name) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string ChangeLogLevelRequestData::doString() const
{
    return fmt::format("log_level={} logger_name={} initiator=0x{:x} timeout_ms={}", log_level, logger_name, initiator, timeout_ms);
}

}
