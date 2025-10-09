#pragma once

#include <Cluster/SchemaRecord/SchemaContextProvider.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{
namespace Streaming
{
class GlobalSchemaProvider final : public cluster::SchemaProvider
{
public:
    const DB::Block & getSchema(const std::string & stream, const DB::UUID & stream_id, uint16_t schema_version) const override;

private:
    /// mutable std::mutex mutex;
    mutable std::unordered_map<DB::UUID, std::unordered_map<uint16_t, DB::Block>> schemas;
};

/// Default schema context just deserialize what has appended
class GlobalSchemaContextProvider : public cluster::SchemaContextProvider
{
public:
    const cluster::SchemaContext & getSchemaContext(const std::string & stream, const DB::UUID & stream_id) const override;

private:
    mutable std::mutex mutex;
    mutable absl::flat_hash_map<DB::UUID, std::pair<std::unique_ptr<cluster::SchemaProvider>, cluster::SchemaContext>> schema_contexts;
};

}
}