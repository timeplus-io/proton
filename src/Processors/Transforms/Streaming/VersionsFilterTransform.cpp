#include <Processors/Transforms/Streaming/VersionsFilterTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/ChooseHashMethod.h>
#include <base/ClockUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Streaming
{
VersionsFilterTransform::VersionsFilterTransform(
    const DB::Block & input_header,
    const DB::Block & output_header,
    std::vector<std::string> key_column_names,
    const std::string & version_column_name,
    bool backfill_key_unique_)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::VersionsFilterTransformID)
    , backfill_key_unique(backfill_key_unique_)
    , output_chunk_header(outputs.front().getHeader().getColumns(), 0)
    , last_log_ts(MonotonicMilliseconds::now())
    , logger(getLogger("VersionsFilterTransform"))
{
    chassert(!key_column_names.empty());
    chassert(!version_column_name.empty());
    chassert(input_header.has(version_column_name));

    output_column_positions.reserve(output_header.columns());
    for (const auto & col : output_header)
        output_column_positions.push_back(input_header.getPositionByName(col.name));

    ColumnRawPtrs key_columns;
    key_columns.reserve(key_column_names.size());
    key_column_positions.reserve(key_column_names.size());
    for (const auto & key_col : key_column_names)
    {
        key_column_positions.push_back(input_header.getPositionByName(key_col));
        key_columns.push_back(input_header.getByPosition(key_column_positions.back()).column.get());
    }

    version_column_position = input_header.getPositionByName(version_column_name);
    version_column_serialization = input_header.getByPosition(version_column_position).type->getDefaultSerialization();

    /// init latest_version_map hash table
    auto [hash_type, key_sizes_] = chooseHashMethod(key_columns);
    latest_version_map.create(hash_type);
    key_sizes.swap(key_sizes_);

    LOG_INFO(
        logger,
        "Prepare versions filtering by keys_num={}, keys_size={} (each key size: {}), input_header={}",
        key_sizes.size(),
        std::accumulate(key_sizes.begin(), key_sizes.end(), static_cast<size_t>(0)),
        fmt::join(key_sizes, ", "),
        input_header.dumpStructure());
}

void VersionsFilterTransform::transform(Chunk & chunk)
{
    /// Propagate empty chunk since it acts like a heartbeat
    auto rows = chunk.rows();
    auto columns = chunk.detachColumns();
    if (rows)
    {
        /// Constant columns are not supported directly during hashing keys. To make them work anyway, we materialize them.
        Columns materialized_columns;
        materialized_columns.reserve(key_column_positions.size() + 1);

        ColumnRawPtrs key_columns;
        key_columns.reserve(key_column_positions.size());

        for (auto key_col_pos : key_column_positions)
        {
            /// Materialize Sparse/Const/LowCardinality columns
            materialized_columns.push_back(columns[key_col_pos]->convertToFullIfNeeded());
            key_columns.push_back(materialized_columns.back().get());
        }

        materialized_columns.push_back(columns[version_column_position]->convertToFullIfNeeded());
        const auto & version_column = *materialized_columns.back();

        switch (latest_version_map.type)
        {
#define M(TYPE) \
    case HashType::TYPE: \
        doFilter<typename KeyGetterForType<HashType::TYPE, std::remove_reference_t<decltype(*latest_version_map.TYPE)>>::Type>( \
            rows, columns, key_columns, version_column, *latest_version_map.TYPE); \
        break;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }

        chunk.setColumns(std::move(columns), rows);
    }
    else
    {
        /// MarkSource is always generating the historical data start / end mark in a separate and empty chunk
        if (!backfill_done)
        {
            if (!backfill_started)
                backfill_started |= chunk.isHistoricalDataStart();
            else
                backfill_done |= chunk.isHistoricalDataEnd();
        }

        chunk.setColumns(output_chunk_header.cloneEmptyColumns(), 0);
    }

    /// Every 30 seconds, log metrics
    if (auto now = MonotonicMilliseconds::now(); now - last_log_ts > log_metrics_interval_ms)
    {
        size_t hash_total_row_count = latest_version_map.getTotalRowCount();
        size_t hash_buffer_bytes = latest_version_map.getBufferSizeInBytes();
        size_t hash_buffer_cells = latest_version_map.getBufferSizeInCells();

        LOG_INFO(
            logger,
            "Cached latest version hash table metrics: hash_total_rows={} hash_total_bytes={} (total_bytes_in_arena:{} "
            "hash_buffer_bytes:{} hash_buffer_cells:{}); late_rows={};",
            hash_total_row_count,
            hash_buffer_bytes + hash_total_row_count * sizeof(Field) + pool.size(),
            pool.size(),
            hash_buffer_bytes,
            hash_buffer_cells,
            late_rows);

        last_log_ts = now;
    }
}

template <typename KeyGetter, typename Map>
void VersionsFilterTransform::doFilter(
    UInt64 & rows, Columns & columns, const ColumnRawPtrs & key_columns, const IColumn & version_column, Map & map)
{
    KeyGetter key_getter(key_columns, key_sizes, nullptr);

    /// Fast path during backfilling
    if (backfillingNewKeys())
    {
        for (size_t row = 0; row < rows; ++row)
        {
            auto result = key_getter.emplaceKey(map, row, pool);
            chassert(result.isInserted());
            result.getMapped() = version_column[row];
        }

        transformToOutputColumns(columns);
        return;
    }

    size_t original_rows = rows;
    IColumn::Filter filter(original_rows, 1);
    for (size_t row = 0; row < original_rows; ++row)
    {
        auto result = key_getter.emplaceKey(map, row, pool);
        auto & latest_version = result.getMapped();
        auto current_version = version_column[row];
        if (result.isInserted())
        {
            /// This is a new key
            latest_version = std::move(current_version);
        }
        else
        {
            if (current_version == latest_version)
            {
                /// Do nothing
            }
            else if (current_version > latest_version)
            {
                /// Update the new version
                latest_version = std::move(current_version);
            }
            else [[unlikely]]
            {
                /// This is a late row, we can skip it
                filter[row] = 0;
                --rows;
            }
        }
    }

    late_rows += original_rows - rows;

    transformToOutputColumns(columns);

    if (rows == original_rows)
    {
        /// No filtering
    }
    else if (rows != 0)
    {
        for (auto & column : columns)
            column = column->filter(filter, rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->cloneEmpty();
    }
}

void VersionsFilterTransform::transformToOutputColumns(Columns & columns) const
{
    Columns output_columns;
    output_columns.reserve(output_column_positions.size());
    for (auto pos : output_column_positions)
        output_columns.push_back(std::move(columns[pos]));

    columns.swap(output_columns);
}

void VersionsFilterTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        FormatSettings format_settings;
        latest_version_map.serialize(
            /*MappedSerializer*/
            [&](const Field & version, WriteBuffer & wb_) { version_column_serialization->serializeBinary(version, wb_, format_settings); },
            wb);

        DB::writeVarUInt(late_rows, wb);
    });
}

void VersionsFilterTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this]([[maybe_unused]] VersionType version_, ReadBuffer & rb) {
        FormatSettings format_settings;
        latest_version_map.deserialize(
            /*MappedDeserializer*/
            [&](Field & version, Arena &, ReadBuffer & rb_) {
                version_column_serialization->deserializeBinary(version, rb_, format_settings);
            },
            pool,
            rb);

        DB::readVarUInt(late_rows, rb);
    });
}
}
}
