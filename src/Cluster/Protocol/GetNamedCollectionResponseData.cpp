#include <cstddef>
#include <Cluster/Protocol/GetNamedCollectionResponseData.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "IO/VarInt.h"
#include "IO/WriteBuffer.h"


namespace cluster::protocol
{
void GetNamedCollectionResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, version);
    if (!err.hasError())
    {
        DB::writeVarUInt(named_collections.size(), wb);
        for (const auto & named_collection : named_collections)
            named_collection->serialize(wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void GetNamedCollectionResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, version);
    if (!err.hasError())
    {
        size_t num_collections = 0;
        DB::readVarUInt(num_collections, rb);
        named_collections.reserve(num_collections);
        for (size_t i = 0; i < num_collections; ++i)
        {
            auto named_collection = std::make_shared<NamedCollectionDescriptor>();
            named_collection->deserialize(rb, version);
            named_collections.push_back(std::move(named_collection));
        }

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t GetNamedCollectionResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    size_t total_size = err.approximateSerializedSize() + sizeof(uint64_t) + sizeof(replica_leader) + sizeof(sn);
    for (const auto & named_collection : named_collections)
        total_size += named_collection->approximateSerializedSize();
    return total_size;
}

std::string GetNamedCollectionResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format("collections={} replica_leader=0x{:x} sn={}", named_collections.size(), replica_leader, sn);
}
}
