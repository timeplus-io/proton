#pragma once

#include <Cluster/SchemaRecord/SchemaProvider.h>

#include <Core/Block.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
struct SchemaBlock;
using SchemaBlockPtr = std::shared_ptr<SchemaBlock>;

class CachedSchemaProvider : public cluster::SchemaProvider
{
public:
    explicit CachedSchemaProvider(const IStorage * storage_);
    const Block & getSchema(const std::string & /*stream*/, const UUID & /*stream_id*/, uint16_t schema_version) const override;
    const ColumnsDescription & getColumnsDescription(uint16_t schema_version) const override;

private:
    const IStorage * storage;

    /// We will need use smart ptr to hold SchemaBlockPtr since the map can be mutated
    /// during the query and getSchema returns the 'Block &', so we will need make sure
    /// the validity (memory address stability) of the block cached
    mutable std::unordered_map<uint16_t, SchemaBlockPtr> schema_cache;
};

}
