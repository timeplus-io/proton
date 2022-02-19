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
    ContextPtr context_,
    Int32 shard_,
    Int64 offset,
    DWAL::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : SourceWithProgress(header)
    , context(std::move(context_))
    , column_names(header.getNames())
    , header_chunk(header.getColumns(), 0)
    , shard(shard_)
    , consumer(std::move(consumer_))
    , log(log_)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    auto schema(storage_->getInMemoryMetadataPtr()->getSampleBlock());
    reader = std::make_unique<StreamingBlockReader>(std::move(storage_), shard, offset, calculateColumnPositions(header, schema), consumer, log);
    iter = result_chunks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

Chunk StreamingStoreSource::generate()
{
    assert(reader);

    if (isCancelled())
    {
        return {};
    }

    /// std::unique_lock lock(result_blocks_mutex);
    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
        {
            return {};
        }

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

    /// 1) Insert raw blocks to in-memory aggregation table
    /// 2) Select the final result from the aggregated table
    /// 3) Update result_blocks and iterator
    result_chunks.clear();
    result_chunks.reserve(records.size());

    for (auto & record : records)
    {
        Columns columns;
        columns.reserve(header_chunk.getNumColumns());
        Block & block = record->block;
        auto rows = block.rows();

        for (const auto & name : column_names)
            columns.push_back(std::move(block.getByName(name).column));

        for (const auto & item : virtual_time_columns_calc)
        {
            auto ts = item.second(block.info);
            auto time_column = virtual_col_type->createColumnConst(rows, ts);
            if (static_cast<Int32>(columns.size()) > item.first)
                columns.insert(columns.begin() + item.first, time_column);
            else
                columns.push_back(time_column);
        }

        result_chunks.emplace_back(columns, rows);
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
    std::vector<uint16_t> column_positions;
    column_positions.reserve(column_names.size());

    for (Int32 pos = 0, size = column_names.size(); pos < size; ++pos)
    {
        if (column_names[pos] == RESERVED_APPEND_TIME)
            virtual_time_columns_calc.emplace_back(pos, [](const BlockInfo & bi) { return bi.append_time; });
        else if (column_names[pos] == RESERVED_INGEST_TIME)
            virtual_time_columns_calc.emplace_back(pos, [](const BlockInfo & bi) { return bi.ingest_time; });
        else if (column_names[pos] == RESERVED_CONSUME_TIME)
            virtual_time_columns_calc.emplace_back(pos, [](const BlockInfo & bi) { return bi.consume_time; });
        else if (column_names[pos] == RESERVED_PROCESS_TIME)
            virtual_time_columns_calc.emplace_back(pos, [](const BlockInfo &) { return UTCMilliseconds::now(); });
        else
        {
            /// FIXME, schema version
            column_positions.push_back(schema.getPositionByName(column_names[pos]));
        }
    }

    std::sort(virtual_time_columns_calc.begin(), virtual_time_columns_calc.end(), [](const auto & lhs, const auto & rhs) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return lhs.first < rhs.first;
    });

    /// Remove the streaming virtual columns since we will handle it here specially
    for (auto item = virtual_time_columns_calc.rbegin(); item != virtual_time_columns_calc.rend(); ++item)
        column_names.erase(column_names.begin() + item->first);

    if (!virtual_time_columns_calc.empty())
        virtual_col_type = header.getByPosition(virtual_time_columns_calc[0].first).type;

    if (context->getSettingsRef().record_consume_batch_count != 0)
        record_consume_batch_count = context->getSettingsRef().record_consume_batch_count;

    if (context->getSettingsRef().record_consume_timeout != 0)
        record_consume_timeout = context->getSettingsRef().record_consume_timeout;

    return column_positions;
}
}
