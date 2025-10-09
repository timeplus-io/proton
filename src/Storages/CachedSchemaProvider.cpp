#include <Storages/CachedSchemaProvider.h>

#include <Storages/IStorage.h>

namespace DB
{

CachedSchemaProvider::CachedSchemaProvider(const IStorage * storage_) : storage(storage_)
{
    chassert(storage != nullptr);
}

const Block & CachedSchemaProvider::getSchema(const std::string & /*stream*/, const UUID & /*stream_id*/, uint16_t schema_version) const
{
    if (auto it = schema_cache.find(schema_version); it != schema_cache.end())
        return it->second;

    return schema_cache.emplace(schema_version, storage->getSchemaByVersion(schema_version)).first->second;
}

}
