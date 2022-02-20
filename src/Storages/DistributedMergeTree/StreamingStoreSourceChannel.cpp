#include "StreamingStoreSourceChannel.h"
#include "StreamingStoreSourceMultiplexer.h"

#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
StreamingStoreSourceChannel::StreamingStoreSourceChannel(
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
    Block header,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr query_context_)
    : SourceWithProgress(header)
    , id(sequence_id++)
    , multiplexer(std::move(multiplexer_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , query_context(std::move(query_context_))
    , records_queue(1000)
    , header_chunk(header.getColumns(), 0)
    , column_positions(header.columns(), 0)
    , virtual_time_columns_calc(header.columns(), nullptr)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    calculateColumnPositions(header, metadata_snapshot->getSampleBlock());
}

std::atomic<uint32_t> StreamingStoreSourceChannel::sequence_id = 0;

StreamingStoreSourceChannel::~StreamingStoreSourceChannel()
{
    multiplexer->removeChannel(id);
}

Chunk StreamingStoreSourceChannel::generate()
{
    if (isCancelled())
        return {};

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

void StreamingStoreSourceChannel::readAndProcess()
{
    DWAL::RecordPtrs records;
    auto got_records = records_queue.tryPop(records, 100);
    if (!got_records)
        return;

    /// Got shutdown signal
    if (records.empty())
    {
        cancel();
        return;
    }

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

        /// Block in channel shall always contain full columns
        assert(block.columns() == total_physical_columns_in_schema);

        for (auto pos : column_positions)
        {
            if (pos < total_physical_columns_in_schema)
            {
                /// We can't move the column to columns
                /// since the records can be shared among multiple threads
                columns.push_back(block.getByPosition(pos).column);
            }
            else
            {
                assert(virtual_time_columns_calc[pos - total_physical_columns_in_schema]);
                auto ts = virtual_time_columns_calc[pos - total_physical_columns_in_schema](block.info);
                auto time_column = virtual_col_type->createColumnConst(rows, ts);
                columns.push_back(std::move(time_column));
            }
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

void StreamingStoreSourceChannel::add(DWAL::RecordPtrs records)
{
    auto added = records_queue.emplace(std::move(records));
    assert(added);
    (void)added;
}

void StreamingStoreSourceChannel::calculateColumnPositions(const Block & header, const Block & schema)
{
    /// FIXME, what about materialized column ?
    total_physical_columns_in_schema = schema.columns();

    /// Calculate column positions in schema
    /// If column is a virtual column, assign it with a virtual position:
    /// its position in header + schema.columns()
    /// For example, header contain 7 columns. Positions 0, 3, 6 in header are virtual columns,
    /// the rest positions are physical and mapped to 3, 1, 5, 8 in schema.
    /// column positions will look like below
    /// [0 + 11, 3, 1, 3 + 11, 5, 8, 6 + 11]
    /// virtual_time_columns_calc vector will look like
    /// [lambda1, nullptr, nullptr, lambda2, nullptr, nullptr, lambda3]
    /// We calculate these column positions and lambda vector for simplify the logic and
    /// fast processing in readAndProcess since we don't need index by column name any more
    /// FIXME, replicate this logic to StreamingStoreSource or share the log
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
}
}
