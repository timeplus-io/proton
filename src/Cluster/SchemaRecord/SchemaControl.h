#pragma once

#include <Compression/CompressionInfo.h>

namespace cluster
{
struct SchemaRecord;
using SchemaRecordPtr = std::shared_ptr<SchemaRecord>;

namespace ctrl
{

SchemaRecordPtr createAlterPartitionRecord(const std::string & partition_command, const std::string & idempotent_key, uint16_t version);

SchemaRecordPtr createTruncateRecord(const std::string & query, const std::string & idempotent_key, uint16_t version);

SchemaRecordPtr createSecondaryIndexRebuildRecord(
    const std::string & index_name, bool truncate_index, bool wait_meta_change, const std::string & idempotent_key, uint16_t version);

SchemaRecordPtr createDeleteRowsRecord(const std::string & predicates, const std::string & idempotent_key, uint16_t version);

SchemaRecordPtr createDropIndexRecord(const std::string & drop_index_command, const std::string & idempotent_key, uint16_t version);

SchemaRecordPtr createOptimizeRecord(
    const std::string & partition,
    bool final,
    bool deduplicate,
    const DB::Names & deduplicate_columns,
    const std::string & idempotent_key,
    uint16_t version);

DB::Block deserializeBlock(
    DB::ReadBuffer & rb, cluster::protocol::OpCode opcode, DB::CompressionMethodByte compression_codec, uint16_t schema_version);

}

}
