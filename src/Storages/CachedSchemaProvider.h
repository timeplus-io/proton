#pragma once

#include <Cluster/SchemaRecord/SchemaProvider.h>

#include <Core/Block.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class CachedSchemaProvider : public cluster::SchemaProvider
{
public:
    explicit CachedSchemaProvider(const IStorage * storage_);
    const Block & getSchema(const std::string & /*stream*/, const UUID & /*stream_id*/, uint16_t schema_version) const override;

private:
    const IStorage * storage;
    mutable std::unordered_map<uint16_t, Block> schema_cache;
};

}
