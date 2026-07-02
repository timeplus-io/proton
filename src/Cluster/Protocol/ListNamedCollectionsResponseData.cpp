#include <Cluster/Protocol/ListNamedCollectionsResponseData.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <IO/WriteHelpers.h>
#include "IO/ReadHelpers.h"
#include <fmt/ranges.h>


namespace cluster::protocol
{
void ListNamedCollectionsResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, version);
    if (!err.hasError())
    {
        DB::writeVarUInt(collection_names.size(), wb);
        for (const auto & name : collection_names)
            DB::writeStringBinary(name, wb);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void ListNamedCollectionsResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, version);
    if (!err.hasError())
    {
        size_t num_collections = 0;
        DB::readVarUInt(num_collections, rb);
        collection_names.resize(num_collections);
        for (auto & name : collection_names)
            DB::readStringBinary(name, rb);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t ListNamedCollectionsResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    size_t total_size = err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn) + sizeof(size_t);
    for (const auto & collection : collection_names)
        total_size += approximateSerializedSizeOf(collection);
    return total_size;
}

std::string ListNamedCollectionsResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format("named_collections=[{}] replica_leader=0x{:x} sn={}", fmt::join(collection_names, ", "), replica_leader, sn);
}
}
