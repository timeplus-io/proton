#include <Cluster/Protocol/MaterializedViewAssignment.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/ClockUtils.h>

namespace cluster::protocol
{
MaterializedViewAssignment::MaterializedViewAssignment(
    cluster::Stream && mv_,
    cluster::NodeID assign_node_,
    cluster::NodeID worker_node_,
    const cluster::NodeUUID & worker_node_uuid_,
    cluster::NodeID reassign_from_)
    : mv(std::move(mv_))
    , assign_node(assign_node_)
    , reassign_from(reassign_from_)
    , worker_node(worker_node_)
    , worker_node_uuid(worker_node_uuid_)
    , timestamp(DB::UTCMilliseconds::now())
{
}

void MaterializedViewAssignment::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 48) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    mv.serialize(wb, Nulls::NullVersion);
    DB::writeVarUInt(assign_node, wb);
    DB::writeVarUInt(reassign_from, wb);
    DB::writeVarUInt(worker_node, wb);
    DB::writeVarUInt(worker_node, wb);
    DB::writeBinary(worker_node_uuid, wb);
    DB::writeVarInt(timestamp, wb);
}

void MaterializedViewAssignment::deserialize(DB::ReadBuffer & rb, uint16_t /*version_*/)
{
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint64_t>(schema_data_version & 0xFFFF'FFFF'FFFF);

    mv.deserialize(rb, Nulls::NullVersion);
    DB::readVarUInt(assign_node, rb);
    DB::readVarUInt(reassign_from, rb);
    DB::readVarUInt(worker_node, rb);
    DB::readVarUInt(worker_node, rb);
    DB::readBinary(worker_node_uuid, rb);
    DB::readVarInt(timestamp, rb);
}

size_t MaterializedViewAssignment::approximateSerializedSize() const noexcept
{
    return sizeof(data_version) + mv.approximateSerializedSize() + sizeof(assign_node) + sizeof(reassign_from) + sizeof(worker_node)
        + sizeof(worker_node_uuid) + sizeof(timestamp);
}

std::string MaterializedViewAssignment::string() const
{
    return fmt::format(
        "data_version={} {} assign_node=0x{:x} reassign_from=0x{:x} worker_node=0x{:x} worker_node_uuid={} timestamp={}",
        data_version,
        mv.string(),
        assign_node,
        reassign_from,
        worker_node,
        DB::toString(worker_node_uuid),
        timestamp);
}

}
