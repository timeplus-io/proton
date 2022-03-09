#include "StreamingStoreSourceBase.h"

#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
StreamingStoreSourceBase::StreamingStoreSourceBase(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr query_context_)
    : SourceWithProgress(header)
    , metadata_snapshot(std::move(metadata_snapshot_))
    , query_context(std::move(query_context_))
    , header_chunk(header.getColumns(), 0)
    , column_positions(header.columns(), 0)
    , virtual_time_columns_calc(header.columns(), nullptr)
{
    /// FIXME, when we have multi-version of schema, the header and the schema may be mismatched
    calculateColumnPositions(header, metadata_snapshot_->getSampleBlock());

    iter = result_chunks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

Chunk StreamingStoreSourceBase::generate()
{
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

void StreamingStoreSourceBase::calculateColumnPositions(const Block & header, const Block & schema)
{
    total_physical_columns_in_schema = schema.columns();

    physical_column_positions_to_read.reserve(header.columns());

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
            physical_column_positions_to_read.push_back(column_positions[pos]);
        }

        ++pos;
    }

    /// Clients like to read virtual columns only, add `_tp_time`, then we know how many rows
    if (physical_column_positions_to_read.empty())
        physical_column_positions_to_read.push_back(schema.getPositionByName(RESERVED_EVENT_TIME));

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
