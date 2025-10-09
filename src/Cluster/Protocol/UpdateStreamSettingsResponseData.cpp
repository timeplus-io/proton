#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/UpdateStreamSettingsResponseData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void UpdateStreamSettingsResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(replica_leader, wb);
    DB::writeVarInt(sn, wb);
}

void UpdateStreamSettingsResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(replica_leader, rb);
    DB::readVarInt(sn, rb);
}

size_t UpdateStreamSettingsResponseData::approximateSerializedSize() const noexcept
{
    return err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

std::string UpdateStreamSettingsResponseData::doString() const
{
    return fmt::format("{} replica_leader={} sn={}", err.string(), replica_leader, sn);
}

}
