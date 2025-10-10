#include <Cluster/Protocol/CreateNamedCollectionRequestData.h>

#include <Cluster/Common/serde.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace cluster::protocol
{
void CreateNamedCollectionRequestData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    DB::writeStringBinary(collection_name, wb);
    chassert(collection);
    collection->serialize(wb, version);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);

    serializeEnum(exists_op, wb);
    DB::writeStringBinary(requested_by, wb);
    DB::writeVarInt(requested_ts, wb);
}

void CreateNamedCollectionRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    DB::readStringBinary(collection_name, rb);
    collection = std::make_shared<NamedCollectionDescriptor>();
    collection->deserialize(rb, version);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);

    exists_op = deserializeEnum<ExistsOperation>(rb);
    DB::readStringBinary(requested_by, rb);
    DB::readVarInt(requested_ts, rb);
}

std::string CreateNamedCollectionRequestData::doString() const
{
    return fmt::format(
        "name={} collection={{{}}} initiator=0x{{:x}} timeout_ms={} exists_op={} requested_by={} requested_ts={}",
        collection_name,
        collection->string(),
        initiator,
        timeout_ms,
        magic_enum::enum_name(exists_op),
        requested_by,
        requested_ts);
}
}
