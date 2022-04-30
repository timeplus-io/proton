#include "StreamingStoreSource.h"
#include "StorageStream.h"
#include "StreamingBlockReaderKafka.h"
#include "StreamingBlockReaderNativeLog.h"

#include <Interpreters/Context.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <KafkaLog/KafkaWALPool.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>

namespace DB
{
StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<IStorage> storage_,
    const Block & header,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    Int32 shard_,
    Int64 sn,
    Poco::Logger * log_)
    : StreamingStoreSourceBase(header, storage_snapshot_, std::move(context_)), shard(shard_), log(log_)
{
    if (query_context->getSettingsRef().record_consume_batch_count != 0)
        record_consume_batch_count = query_context->getSettingsRef().record_consume_batch_count;

    if (query_context->getSettingsRef().record_consume_timeout != 0)
        record_consume_timeout = query_context->getSettingsRef().record_consume_timeout;

    auto stream_storage = static_cast<StorageStream *>(storage_.get());
    if (stream_storage->isLogstoreKafka())
    {
        auto & kpool = klog::KafkaWALPool::instance(query_context);
        assert(kpool.enabled());
        auto consumer = kpool.getOrCreateStreaming(stream_storage->streamingStorageClusterId());
        assert(consumer);
        kafka_reader = std::make_unique<StreamingBlockReaderKafka>(
            std::move(storage_), shard, sn, physical_column_positions_to_read, std::move(consumer), log);
    }
    else
    {
        auto fetch_buffer_size = query_context->getSettingsRef().fetch_buffer_size;
        fetch_buffer_size = std::min<UInt64>(64 * 1024 * 1024, fetch_buffer_size);
        nativelog_reader = std::make_unique<StreamingBlockReaderNativeLog>(
            std::move(storage_),
            shard_,
            sn,
            record_consume_timeout,
            fetch_buffer_size,
            /*schema_provider*/ nullptr,
            /*schema_version*/ 0,
            physical_column_positions_to_read,
            log);
    }
}

nlog::RecordPtrs StreamingStoreSource::read()
{
    if (nativelog_reader)
        return nativelog_reader->read();
    else
        return kafka_reader->read(record_consume_batch_count, record_consume_timeout);
}

void StreamingStoreSource::readAndProcess()
{
    auto records = read();
    if (records.empty())
        return;

    result_chunks.clear();
    result_chunks.reserve(records.size());

    auto pos_size = column_positions.size();
    for (auto & record : records)
    {
        if (record->empty())
            continue;

        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->getBlock();
        auto rows = block.rows();

        assert(pos_size >= block.columns());

        if (hasObjectColumns())
            fillAndUpdateObjects(block);

        /// Pos Range: [0, ..., virtual_columns_pos_begin, ..., subcolumns_pos_begin, ...)
        for (size_t index = 0, physical_col_index = 0, subcolumn_index = 0; index < pos_size; ++index)
        {
            auto pos_in_schema = column_positions[index];
            if (pos_in_schema < virtual_columns_pos_begin)
            {
                /// At current result column index, it is expecting a physical column
                columns.push_back(block.getByPosition(physical_col_index).column);
                ++physical_col_index;
            }
            else if (pos_in_schema >= subcolumns_pos_begin)
            {
                /// It's a subcolumn, the parent (physical) column pos in schema is `pos_in_schema - subcolumns_pos_begin`
                columns.push_back(getSubcolumnFromblock(block, pos_in_schema - subcolumns_pos_begin, subcolumns_to_read[subcolumn_index]));
                ++subcolumn_index;
            }
            else
            {
                /// The current column to return is a virtual column which needs be calculated lively
                assert(virtual_time_columns_calc[pos_in_schema - virtual_columns_pos_begin]);
                auto ts = virtual_time_columns_calc[pos_in_schema - virtual_columns_pos_begin](block.info);
                auto time_column = virtual_col_type->createColumnConst(rows, ts);
                columns.push_back(std::move(time_column));
            }
        }

        result_chunks.emplace_back(std::move(columns), rows);
        if (likely(block.info.append_time > 0))
        {
            auto chunk_info = std::make_shared<ChunkInfo>();
            chunk_info->ctx.setAppendTime(block.info.append_time);
            result_chunks.back().setChunkInfo(std::move(chunk_info));
        }
    }
    iter = result_chunks.begin();
}
}
