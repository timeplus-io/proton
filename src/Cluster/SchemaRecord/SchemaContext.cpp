#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

namespace cluster
{
SchemaContext::SchemaContext() : SchemaContext(EmptySchemaProvider::instance())
{
}

SchemaContext::SchemaContext(const SchemaProvider & provider)
    : schema_provider(&provider), schema_version_requested(SchemaRecord::ANY_SCHEMA_VERSION)
{
}

SchemaContext::SchemaContext(const SchemaProvider & schema_provider_, const std::string & stream_, const DB::UUID & stream_id_)
    : schema_provider(&schema_provider_), stream(stream_), stream_id(stream_id_), schema_version_requested(SchemaRecord::ANY_SCHEMA_VERSION)
{
}

SchemaContext::SchemaContext(
    const SchemaProvider & schema_provider_,
    const std::string & stream_,
    const DB::UUID & stream_id_,
    uint16_t schema_version_requested_,
    SourceColumnsDescription::PhysicalColumnPositions column_positions_requested_)
    : schema_provider(&schema_provider_)
    , stream(stream_)
    , stream_id(stream_id_)
    , schema_version_requested(schema_version_requested_)
    , column_positions_requested(std::move(column_positions_requested_))
{
}

bool SchemaContext::serializableCompatible(uint16_t on_storage_schema_version) const
{
    if (on_storage_schema_version == schema_version_requested)
        return true;

    if (schema_version_requested == SchemaRecord::ANY_SCHEMA_VERSION)
    {
        chassert(column_positions_requested.positions.empty());
        return true;
    }

    const auto & on_storage_schema = getSchema(on_storage_schema_version);
    auto on_storage_schema_columns = on_storage_schema.columns();

    const auto & query_schema = getRequestedSchema();
    chassert(query_schema.columns() >= column_positions_requested.positions.size());

    for (auto pos : column_positions_requested.positions)
    {
        /// If requested schema is newer, still treat it is compatible since the missing (new) columns will be filled later with default
        if (pos >= on_storage_schema_columns)
            continue;

        const auto & on_storage_column = on_storage_schema.getByPosition(pos);
        const auto & query_column = query_schema.getByPosition(pos);

        /// Check if either type is null - this can happen during schema evolution
        if (!on_storage_column.type || !query_column.type)
            return false;

        /// What about name ? rename is ok ?
        if (!on_storage_column.type->equals(*query_column.type))
            return false;
    }

    return true;
}

}
