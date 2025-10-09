#include <Interpreters/Streaming/GlobalSchemaProvider.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace Streaming
{
const Block & GlobalSchemaProvider::getSchema(const std::string & stream, const DB::UUID & stream_id, uint16_t schema_version) const
{
    {
        /// std::scoped_lock lock{mutex};
        auto versioned_schemas_iter = schemas.find(stream_id);
        if (versioned_schemas_iter != schemas.end())
        {
            auto versioned_schema_iter = versioned_schemas_iter->second.find(schema_version);
            if (versioned_schema_iter != versioned_schemas_iter->second.end())
                return versioned_schema_iter->second;
        }
    }

    auto database_and_table = DatabaseCatalog::instance().tryGetByUUID(stream_id);
    if (database_and_table.second != nullptr)
    {
        auto metadata_snapshot = database_and_table.second->getInMemoryMetadataByVersion(schema_version);
        auto schema = metadata_snapshot->getSampleBlock();

        {
            /// std::scoped_lock lock{mutex};
            schemas[stream_id].emplace(schema_version, std::move(schema));
        }
        return schemas[stream_id][schema_version];
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_STREAM, "Stream '{}' with id '{}' doesn't exist", stream, toString(stream_id));
}

const cluster::SchemaContext & GlobalSchemaContextProvider::getSchemaContext(const std::string & stream, const DB::UUID & stream_id) const
{
    std::scoped_lock lock{mutex};

    auto iter = schema_contexts.find(stream_id);
    if (iter != schema_contexts.end())
        return iter->second.second;

    auto schema_provider = std::make_unique<GlobalSchemaProvider>();
    cluster::SchemaContext schema_ctx{*schema_provider, stream, stream_id};
    [[maybe_unused]] auto [inserted_iter, inserted]
        = schema_contexts.emplace(stream_id, std::pair{std::move(schema_provider), std::move(schema_ctx)});
    assert(inserted);
    return inserted_iter->second.second;
}

}
}
