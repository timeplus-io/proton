#include <Cluster/Protocol/CreateDatabaseRequestData.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void CreateDatabaseRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    chassert(database);
    database->serialize(wb, /*version=*/1);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateDatabaseRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    database = std::make_shared<DatabaseDescriptor>();
    database->deserialize(rb, /*version=*/1);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}


size_t CreateDatabaseRequestData::approximateSerializedSize() const noexcept
{
    assert(database);
    return database->approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms);
}

std::string CreateDatabaseRequestData::doString() const
{
    return fmt::format("database={{{}}} initiator=0x{:x} timeout_ms={}", database ? database->string() : "null", initiator, timeout_ms);
}

}
