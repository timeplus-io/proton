#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/SchemaRecord/SchemaProvider.h>
#include <Cluster/SchemaRecord/SourceColumnsDescription.h>

namespace cluster
{
/// When reading streaming records from streaming store, there are several scenarios
/// 1) During query processing, users can select partial of the columns and when the query is planned before it run,
///    we pin the query to a specific schema version of each stream related. And also setup the columns requested.
///    If the query schema version is `compatible` with the on storage schema version, we still deserialize
///    what has appended in the store and drop columns we don't want to have or add back columns which are absent in the
///    streaming record.
///    On the other hand, if the query schema version is not `compatible` with the on storage schema version, we still
///    deserialize by using on storage schema version, and do necessary conversion during query time after we read the data.
///    If the conversion failed, abort the query since it is absolutely in-compatible.
/// 2) In the background process which tails data from streaming store and index them to historical store. In this case,
///    we generally read what we have appended to the stream store with whatever versions it has and may need to convert to the
///    latest schema version the historical store has. This conversion doesn't happened during deserialization but
///    happens in a different background process
/// 3) During ingestion / replication, we need deserialize the data on wire by using whatever schema version it has
///
/// When a query is run, it is bound to a specific schema version for each underlying physical streams (indicated by schema_version_requested_)
/// and the columns of that specific version (indicated by `column_positions_requested`) the query needs to query.
/// SchemaContext is usually initiated once per query for each physical stream underlying and then reused across the whole life cycle
/// of the query, this will have the best efficiency
struct SchemaContext
{
    SchemaContext();

    explicit SchemaContext(const SchemaProvider & provider);

    SchemaContext(const SchemaProvider & schema_provider_, const std::string & stream_, const DB::UUID & stream_id_);

    SchemaContext(
        const SchemaProvider & schema_provider_,
        const std::string & stream_,
        const DB::UUID & stream_id_,
        uint16_t schema_version_requested_,
        SourceColumnsDescription::PhysicalColumnPositions column_positions_requested_);

    const DB::Block & getSchema(uint16_t on_storage_schema_version) const
    {
        return schema_provider->getSchema(stream, stream_id, on_storage_schema_version);
    }

    const DB::Block & getRequestedSchema() const { return schema_provider->getSchema(stream, stream_id, schema_version_requested); }

    /// Check if on storage schema version is compatible with run time query schema version
    bool serializableCompatible(uint16_t on_storage_schema_version) const;

    void setAvoidFillDefaultsForMissingColumns(bool avoid_fill_defaults_for_missing_columns_) const noexcept
    {
        avoid_fill_defaults_for_missing_columns = avoid_fill_defaults_for_missing_columns_;
    }

    const SchemaProvider * schema_provider;
    std::string stream;
    StreamID stream_id = Nulls::NullStreamID;
    /// Query time schema version pinned
    uint16_t schema_version_requested;
    /// Query time requested columns
    SourceColumnsDescription::PhysicalColumnPositions column_positions_requested;

    /// Data in WAL can be partial columns, usually we need refill the missing columns with default values when read
    /// This flag tells deserialize what has been serialized, a.k.a, don't fill default values for missing columns when read.
    /// For example, for schema [a int, b string, c float], [a, b] was inserted in WAL, during deserialization
    /// only [a, b] will be deserialized. Used by coalesced mutable stream
    mutable bool avoid_fill_defaults_for_missing_columns = false;

    constexpr static uint16_t NO_POSITION = std::numeric_limits<uint16_t>::max();
};

}
