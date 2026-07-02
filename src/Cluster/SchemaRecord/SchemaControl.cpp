#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaControl.h>
#include <Cluster/SchemaRecord/SchemaNativeReader.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

#include <Cluster/SchemaRecord/ControlBlockSchemaProvider.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
}

namespace cluster::ctrl
{

namespace
{

/// Control schemas are globally unique and versioned. All of them shall be recorded here
///
/// The first column of all control blocks will be the version column
/// The second column of all control blocks will be the flag column
/// And other application specific columns follow
/// [version][flag][...]
const DB::Block & getAlterPartitionSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "partition_command"}}}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Alter partition control schema version={} is not supported", schema_version);
}

const DB::Block & getTruncateSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "truncate_query"}}}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Truncate control schema version={} is not supported", schema_version);
}

const DB::Block & getSecondaryIndexRebuildSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "index_name"}}}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Rebuild index control schema version={} is not supported", schema_version);
}

const DB::Block & getDeleteRowsSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "predicates"}}}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Delete rows control schema version={} is not supported", schema_version);
}

const DB::Block & getDropIndexSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "drop_index_command"}}}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Drop index control schema version={} is not supported", schema_version);
}

const DB::Block & getOptimizeSchema(uint16_t schema_version)
{
    const static std::unordered_map<uint16_t, DB::Block> versioned_schemas
        = {{1,
            DB::Block{
                {DB::ColumnUInt16::create(), std::make_shared<DB::DataTypeUInt16>(), "_tp_version"},
                {DB::ColumnUInt64::create(), std::make_shared<DB::DataTypeUInt64>(), "_tp_flags"},
                {DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "partition"},
                {DB::ColumnUInt8::create(), std::make_shared<DB::DataTypeUInt8>(), "final"},
                {DB::ColumnUInt8::create(), std::make_shared<DB::DataTypeUInt8>(), "deduplicate"},
                {DB::ColumnArray::create(DB::ColumnString::create()),
                 std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeString>()),
                 "deduplicate_columns"},
            }}};

    auto iter = versioned_schemas.find(schema_version);
    if (iter != versioned_schemas.end())
        return iter->second;

    throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Optimize control schema version={} is not supported", schema_version);
}

/// Control blocks are special blocks used for internal operations like alter partition, truncate etc.
/// Each control block has a standard header:
/// - _tp_version: UInt16 - Schema version of the control block
/// - _tp_flags: UInt64 - Operation specific flags
///
/// Current schema version: 1
/// Supported operations:
/// - AlterPartition: [_tp_version][_tp_flags][partition_command:String]
/// - Truncate: [_tp_version][_tp_flags][truncate_query:String]
/// - RebuildSecondaryIndex: [_tp_version][_tp_flags][index_name:String]
const DB::Block & getSchema(cluster::protocol::OpCode opcode, uint16_t schema_version)
{
    switch (opcode)
    {
        case cluster::protocol::OpCode::AlterPartition:
            return getAlterPartitionSchema(schema_version);
        case cluster::protocol::OpCode::Truncate:
            return getTruncateSchema(schema_version);
        case cluster::protocol::OpCode::RebuildIndex:
            return getSecondaryIndexRebuildSchema(schema_version);
        case cluster::protocol::OpCode::DeleteRows:
            return getDeleteRowsSchema(schema_version);
        case cluster::protocol::OpCode::Optimize:
            return getOptimizeSchema(schema_version);
        case cluster::protocol::OpCode::DropIndex:
            return getDropIndexSchema(schema_version);
        default:
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED, "{} control block schema is not supported yet", magic_enum::enum_name(opcode));
    }
}

}

cluster::SchemaRecordPtr
createAlterPartitionRecord(const std::string & partition_command, const std::string & idempotent_key, uint16_t version)
{
    chassert(version == 1); /// Expand version if future

    DB::Block block = getAlterPartitionSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    columns[1]->insert(static_cast<uint64_t>(0));
    columns[2]->insert(partition_command);
    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::AlterPartition, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

SchemaRecordPtr createTruncateRecord(const std::string & query, const std::string & idempotent_key, uint16_t version)
{
    chassert(version == 1); /// Expand version if future

    DB::Block block = getTruncateSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    columns[1]->insert(static_cast<uint64_t>(0));
    columns[2]->insert(query);
    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::Truncate, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

SchemaRecordPtr createSecondaryIndexRebuildRecord(
    const std::string & index_name, bool truncate_index_first, bool wait_meta_change, const std::string & idempotent_key, uint16_t version)
{
    chassert(version == 1); /// Expand version if future
    /// flags bits
    /// first bit: clean index or not

    DB::Block block = getSecondaryIndexRebuildSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    uint64_t flags = static_cast<uint64_t>(truncate_index_first) | (static_cast<uint64_t>(wait_meta_change) << 1);
    columns[1]->insert(flags);
    columns[2]->insert(index_name);
    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::RebuildIndex, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

SchemaRecordPtr createDeleteRowsRecord(const std::string & predicates, const std::string & idempotent_key, uint16_t version)
{
    chassert(version == 1); /// Expand version if future

    DB::Block block = getDeleteRowsSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    columns[1]->insert(static_cast<uint64_t>(0));
    columns[2]->insert(predicates);
    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::DeleteRows, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

SchemaRecordPtr createDropIndexRecord(const std::string & drop_index_command, const std::string & idempotent_key, uint16_t version)
{
    chassert(version == 1); /// Expand version if future

    DB::Block block = getDropIndexSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    columns[1]->insert(static_cast<uint64_t>(0));
    columns[2]->insert(drop_index_command);
    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::DropIndex, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

SchemaRecordPtr createOptimizeRecord(
    const std::string & partition,
    bool final,
    bool deduplicate,
    const DB::Names & deduplicate_columns,
    const std::string & idempotent_key,
    uint16_t version)
{
    chassert(version == 1); /// Expand version if future

    DB::Block block = getOptimizeSchema(version);
    auto columns = block.mutateColumns();
    columns[0]->insert(version);
    columns[1]->insert(static_cast<uint64_t>(0));
    columns[2]->insert(partition);
    columns[3]->insert(final); /// final
    columns[4]->insert(deduplicate); /// deduplicate

    DB::Array array{deduplicate_columns.begin(), deduplicate_columns.end()};
    columns[5]->insert(array); /// deduplicate_columns

    block.setColumns(std::move(columns));

    auto record = std::make_shared<cluster::SchemaRecord>(cluster::protocol::OpCode::Optimize, std::move(block), version);

    if (!idempotent_key.empty())
        record->setIdempotentKey(idempotent_key);

    return record;
}

DB::Block deserializeBlock(
    DB::ReadBuffer & rb, cluster::protocol::OpCode opcode, DB::CompressionMethodByte compression_codec, uint16_t schema_version)
{
    /// Add version validation
    chassert(schema_version == 1);

    ControlBlockSchemaProvider provider(getSchema(opcode, schema_version));

    SchemaContext schema_ctx(provider);
    schema_ctx.schema_version_requested = schema_version;

    /// Control block is always in NativeInSchemaFull
    if (compression_codec == DB::CompressionMethodByte::NONE)
    {
        SchemaNativeReader reader(schema_version, schema_ctx);
        DB::Block result;
        reader.read(result, rb, /*partial_=*/false);
        return result;
    }
    else
    {
        DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
        SchemaNativeReader reader(schema_version, schema_ctx);
        DB::Block result;
        reader.read(result, compressed_in, /*partial_=*/false);
        return result;
    }
}

}
