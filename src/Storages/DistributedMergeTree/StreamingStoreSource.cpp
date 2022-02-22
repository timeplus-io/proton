#include "StreamingStoreSource.h"
#include "StorageDistributedMergeTree.h"
#include "StreamingBlockReader.h"

#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>

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
    : SourceWithProgress(header)
    , context(std::move(context_))
    , header_chunk(header.getColumns(), 0)
    , column_positions(header.columns(), 0)
    , virtual_time_columns_calc(header.columns(), nullptr)
    , shard(shard_)
    , consumer(std::move(consumer_))
    , log(log_)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    auto schema(metadata_snapshot_->getSampleBlock());
    reader = std::make_unique<StreamingBlockReader>(
        std::move(storage_), shard, offset, calculateColumnPositions(header, schema), consumer, log);
    iter = result_chunks.begin();

    if (context->getSettingsRef().record_consume_batch_count != 0)
        record_consume_batch_count = context->getSettingsRef().record_consume_batch_count;

    if (context->getSettingsRef().record_consume_timeout != 0)
        record_consume_timeout = context->getSettingsRef().record_consume_timeout;

    last_flush_ms = MonotonicMilliseconds::now();
}

Chunk StreamingStoreSource::generate()
{
    assert(reader);

    if (isCancelled())
        return {};

    /// std::unique_lock lock(result_blocks_mutex);
    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks.empty() || iter == result_chunks.end())
        {
            /// Act as a heart beat and flush
            last_flush_ms = MonotonicMilliseconds::now();
            return header_chunk.clone();
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return header_chunk.clone();
    }

    return std::move(*iter++);
}

void StreamingStoreSource::readAndProcess()
{
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

std::vector<uint16_t> StreamingStoreSource::calculateColumnPositions(const Block & header, const Block & schema)
{
    total_physical_columns_in_schema = schema.columns();

    /// Column positions to read from file system
    std::vector<uint16_t> column_positions_to_read;
    column_positions_to_read.reserve(header.columns());

    for (size_t pos = 0; const auto & column : header)
    {
        if (column.name == RESERVED_APPEND_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo & bi) { return bi.append_time; };
            column_positions[pos] = pos + total_physical_columns_in_schema;
        }
        else if (column.name == RESERVED_INGEST_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo & bi) { return bi.ingest_time; };
            column_positions[pos] = pos + total_physical_columns_in_schema;
        }
        else if (column.name == RESERVED_CONSUME_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo & bi) { return bi.consume_time; };
            column_positions[pos] = pos + total_physical_columns_in_schema;
        }
        else if (column.name == RESERVED_PROCESS_TIME)
        {
            virtual_time_columns_calc[pos] = [](const BlockInfo &) { return UTCMilliseconds::now(); };
            column_positions[pos] = pos + total_physical_columns_in_schema;
        }
        else
        {
            /// FIXME, schema version. For non virtual
            column_positions[pos] = schema.getPositionByName(column.name);
            column_positions_to_read.push_back(column_positions[pos]);
        }

        ++pos;
    }

    for (size_t i = 0; i < virtual_time_columns_calc.size(); ++i)
    {
        if (virtual_time_columns_calc[i])
        {
            /// We are assuming all virtual timestamp columns have the same data type
            virtual_col_type = header.getByPosition(i).type;
            break;
        }
    }

    return column_positions_to_read;
}
}
