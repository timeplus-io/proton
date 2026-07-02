#include <Storages/CachedSchemaProvider.h>
#include <Storages/SchemaBlock.h>

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
        return it->second->schema_block;

    auto [it, inserted] = schema_cache.emplace(schema_version, storage->getMetadataByVersion(schema_version));
    chassert(inserted);
    return it->second->schema_block;
}

const ColumnsDescription & CachedSchemaProvider::getColumnsDescription(uint16_t schema_version) const
{
    if (auto it = schema_cache.find(schema_version); it != schema_cache.end())
        return it->second->storage_metadata->getColumns();

    auto [it, inserted] = schema_cache.emplace(schema_version, storage->getMetadataByVersion(schema_version));
    chassert(inserted);
    return it->second->storage_metadata->getColumns();
}

}
