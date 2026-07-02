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
    const Block & getSchema(const std::string & stream, const UUID & stream_id, uint16_t schema_version) const override;

private:
    /// mutable std::mutex mutex;
    mutable std::unordered_map<UUID, std::unordered_map<uint16_t, std::unique_ptr<DB::Block>>> schemas;
};

/// Default schema context just deserialize what has appended
class GlobalSchemaContextProvider : public cluster::SchemaContextProvider
{
public:
    const cluster::SchemaContext & getSchemaContext(const std::string & stream, const UUID & stream_id) const override;

private:
    mutable std::mutex mutex;

    struct SchemaProviderAndContext
    {
        std::unique_ptr<cluster::SchemaProvider> provider;
        cluster::SchemaContext ctx;
    };
    using SchemaProviderAndContextPtr = std::unique_ptr<SchemaProviderAndContext>;

    /// Storage UUID -> Schema Context
    mutable absl::flat_hash_map<UUID, SchemaProviderAndContextPtr> schema_contexts;
};

}
}
