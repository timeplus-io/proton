#include "StreamingStoreSource.h"
#include "StreamingBlockReader.h"

#include <Interpreters/Context.h>

namespace DB
{
StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<IStorage> storage_,
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    Int32 shard_,
    Int64 offset,
    DWAL::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : StreamingStoreSourceBase(header, metadata_snapshot_, std::move(context_))
    , shard(shard_)
    , consumer(std::move(consumer_))
    , log(log_)
{
    reader = std::make_unique<StreamingBlockReader>(
        std::move(storage_), shard, offset, physical_column_positions_to_read, consumer, log);

    if (query_context->getSettingsRef().record_consume_batch_count != 0)
        record_consume_batch_count = query_context->getSettingsRef().record_consume_batch_count;

    if (query_context->getSettingsRef().record_consume_timeout != 0)
        record_consume_timeout = query_context->getSettingsRef().record_consume_timeout;
}

void StreamingStoreSource::readAndProcess()
{
    assert(reader);

    auto records = reader->read(record_consume_batch_count, record_consume_timeout);
    if (records.empty())
        return;

    result_chunks.clear();
    result_chunks.reserve(records.size());

    for (auto & record : records)
    {
        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->block;
        auto rows = block.rows();

        assert (column_positions.size() >= block.columns());

        for (size_t index = 0; auto & column : block)
        {
            if (column_positions[index] > total_physical_columns_in_schema)
            {
                /// The current column to return is a virtual column which needs be calculated lively
                assert(virtual_time_columns_calc[column_positions[index] - total_physical_columns_in_schema]);
                auto ts = virtual_time_columns_calc[column_positions[index] - total_physical_columns_in_schema](block.info);
                auto time_column = virtual_col_type->createColumnConst(rows, ts);
                columns.push_back(std::move(time_column));
            }
            columns.push_back(std::move(column.column));
            ++index;
        }

        result_chunks.emplace_back(std::move(columns), rows);
        if (likely(record->block.info.append_time > 0))
        {
            auto chunk_info = std::make_shared<ChunkInfo>();
            chunk_info->ctx.setAppendTime(record->block.info.append_time);
            result_chunks.back().setChunkInfo(std::move(chunk_info));
        }
    }
    iter = result_chunks.begin();
}
}
