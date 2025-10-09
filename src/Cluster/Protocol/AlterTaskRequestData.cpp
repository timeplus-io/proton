#include <Cluster/Protocol/AlterTaskRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void AlterTaskRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeBinary(ns, wb);
    DB::writeBinary(name, wb);
    DB::writeBinary(id, wb);
    DB::writeVarUInt(data_version, wb);
    DB::writeVarUInt(static_cast<uint8_t>(type), wb);
    DB::writeVarUInt(status, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
    DB::writeStringBinary(requested_by, wb);
    DB::writeVarInt(requested_ts, wb);
}

void AlterTaskRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readBinary(ns, rb);
    DB::readBinary(name, rb);
    DB::readBinary(id, rb);
    DB::readVarUInt(data_version, rb);
    DB::readVarUInt(reinterpret_cast<uint8_t &>(type), rb);
    DB::readVarUInt(status, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
    DB::readStringBinary(requested_by, rb);
    DB::readVarInt(requested_ts, rb);
}

std::string AlterTaskRequestData::doString() const
{
    std::string description;
    switch (type)
    {
        case AlterType::STATUS:
            description = fmt::format("status={}", initiator);
            break;
    }

    return fmt::format(
        "ns={} name={} {} initiator=0x{:x} timeout_ms={} requested_by={} requested_ts={}",
        ns,
        name,
        description,
        initiator,
        timeout_ms,
        requested_by,
        requested_ts);
}
}
