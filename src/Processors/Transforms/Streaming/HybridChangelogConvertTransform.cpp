#include <Processors/Transforms/Streaming/HybridChangelogConvertTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Streaming/ChooseHybridHashMethod.h>
#include <Interpreters/Streaming/OneRow.h>
#include <base/ClockUtils.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <ranges>

namespace DB
{
namespace Streaming
{

HybridChangelogConvertTransform::HybridChangelogConvertTransform(
    const Block & input_header,
    const Block & output_header,
    std::vector<std::string> key_column_names,
    const std::string & version_column_name,
    std::string spill_dir,
    size_t max_hot_key_count,
    bool backfill_key_unique_)
    : IProcessor({input_header}, {output_header}, ProcessorID::HybridChangelogConvertTransformID)
    , backfill_key_unique(backfill_key_unique_)
    , output_chunk_header(outputs.front().getHeader().getColumns(), 0)
    , last_log_ts(MonotonicMilliseconds::now())
    , logger(getLogger("HybridChangelogConvertTransform"))
{
    chassert(!key_column_names.empty());
    chassert(!input_header.has(ProtonConsts::RESERVED_DELTA_FLAG));
    chassert(output_header.getByPosition(output_header.columns() - 1).name == ProtonConsts::RESERVED_DELTA_FLAG);

    output_column_positions.reserve(output_header.columns());

    for (const auto & col : output_header)
    {
        if (col.name != ProtonConsts::RESERVED_DELTA_FLAG)
            output_column_positions.push_back(input_header.getPositionByName(col.name));
    }

    chassert(output_column_positions.size() == output_header.columns() - 1);

    DataTypes key_column_types;
    key_column_types.reserve(key_column_names.size());
    key_column_positions.reserve(key_column_names.size());

    for (const auto & key_col : key_column_names)
    {
        key_column_positions.push_back(input_header.getPositionByName(key_col));
        key_column_types.push_back(input_header.getByPosition(key_column_positions.back()).type);
    }

    output_key_column_positions.reserve(key_column_positions.size());
    output_value_column_positions.reserve(output_column_positions.size());
    value_column_positions.reserve(output_column_positions.size());
    for (auto output_col_pos : output_column_positions)
    {
        if (auto iter = std::ranges::find(key_column_positions, output_col_pos); iter != key_column_positions.end())
        {
            output_key_column_positions.push_back(output_col_pos);
        }
        else
        {
            output_value_column_positions.push_back(output_col_pos);
            value_column_positions.push_back(output_col_pos);
        }
    }

    chassert(output_key_column_positions.size() + output_value_column_positions.size() == output_column_positions.size());

    if (!version_column_name.empty())
    {
        input_version_column_position = input_header.getPositionByName(version_column_name);
        auto it = std::ranges::find(value_column_positions, *input_version_column_position);
        if (it != value_column_positions.end())
        {
            version_column_position_in_row = std::distance(value_column_positions.begin(), it);
        }
        else
        {
            /// If value columns don't contain the version column, and version column exists, cache it to the end of the row
            version_column_position_in_row = value_column_positions.size();
            value_column_positions.push_back(*input_version_column_position);
        }
    }

    createHashTable(key_column_types, std::move(spill_dir), max_hot_key_count);

    LOG_INFO(
        logger,
        "Prepare converting changelog by keys_num={}, keys_size={} (each key size: {}), input_header={}",
        key_sizes.size(),
        std::accumulate(key_sizes.begin(), key_sizes.end(), static_cast<size_t>(0)),
        fmt::join(key_sizes, ", "),
        input_header.dumpStructure());
}

IProcessor::Status HybridChangelogConvertTransform::prepare()
{
    /// std::scoped_lock lock(mutex);
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Push if we have data
    if (!output_chunks.empty())
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
    }

    Status status = Status::NeedData;
    if (!output_chunks.empty())
    {
        /// First drain what we have so far
        input.setNotNeeded();
        status = Status::PortFull;
    }
    else if (input.isFinished())
    {
        output.finish();
        status = Status::Finished;
    }
    else
    {
        chassert(!input_data.chunk);

        /// We have drain output_chunks
        input.setNeeded();

        if (input.hasData())
        {
            input_data = input.pullData(/*set_not_needed= */ false);
            if (input_data.exception)
                /// No more data needed. Exception will be thrown (or swallowed) later.
                input.setNotNeeded();

            status = Status::Ready;
        }
        else
            status = Status::NeedData;
    }

    return status;
}

void HybridChangelogConvertTransform::work()
{
    if (input_data.exception)
        std::rethrow_exception(input_data.exception);

    const auto & chunk = input_data.chunk;

    auto start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += chunk.bytes();
    metrics.processed_rows += chunk.rows();
    SCOPE_EXIT({ metrics.processed_time_ns += MonotonicNanoseconds::now() - start_ns; });

    if (auto ckpt_ctx = chunk.getCheckpointContext(); ckpt_ctx)
    {
        chassert(chunk.rows() == 0);
        checkpoint(ckpt_ctx);
        transformEmptyChunk();
        return;
    }

    /// Propagate empty chunk since it acts like a heartbeat
    auto rows = chunk.rows();
    if (rows)
    {
        /// Constant columns are not supported directly during hashing keys. To make them work anyway, we materialize them.
        Columns materialized_columns;
        materialized_columns.reserve(key_column_positions.size());

        ColumnRawPtrs key_columns;
        key_columns.reserve(key_column_positions.size());

        const auto & columns = chunk.getColumns();
        for (auto key_col_pos : key_column_positions)
        {
            /// Materialize Sparse/Const/LowCardinality columns
            materialized_columns.push_back(columns[key_col_pos]->convertToFullIfNeeded());
            key_columns.push_back(materialized_columns.back().get());
        }

        switch (index.getType())
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            case HybridHashType::key_hashed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "HybridHashTable shall have a key column, but empty or without or hashed key type is found");

#define M(TYPE, IS_TWO_LEVEL) \
    case HybridHashType::TYPE: \
        if (has_nullable_key) \
            retractAndIndex<HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/true>>(rows, key_columns, *index.TYPE); \
        else \
            retractAndIndex<HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/false>>(rows, key_columns, *index.TYPE); \
        break;
                APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
        }
    }
    else
    {
        transformEmptyChunk();
    }

    /// Every 30 seconds, log metrics
    if (auto now = MonotonicMilliseconds::now(); now - last_log_ts > log_metrics_interval_ms)
    {
        LOG_INFO(logger, "Hybrid hashtable metrics={{{}}} approximate_keys={}", index.metrics().string(), index.approximateCount());
        last_log_ts = now;
    }
}

template <typename KeyGetter, typename Table>
void HybridChangelogConvertTransform::retractAndIndex(size_t rows, const ColumnRawPtrs & key_columns, Table & table)
{
    /// std::scoped_lock lock(mutex);

    Chunk input_chunk{std::move(input_data.chunk)};
    chassert(!input_data.chunk);
    const auto & input_columns = input_chunk.getColumns();

    KeyGetter key_getter(key_columns, key_sizes);

    HybridEmplaceResults emplace_results;
    {
        std::vector<typename KeyGetter::KeyType> keys;
        keys.reserve(rows);

        for (size_t row = 0; row < rows; ++row)
            keys.emplace_back(key_getter.getKeyHolder(row));

        auto results = backfillingNewKeys() ? table.emplaceNewKeys(keys) : table.emplaceKeys(keys);
        if (results.hasError())
            throw Exception::createRuntime(results.errorCode(), results.errorString());

        emplace_results.results.swap(results.results);
    }

    /// Fast path during backfilling
    if (backfillingNewKeys())
    {
        if (!value_column_positions.empty())
        {
            for (size_t row = 0; row < rows; ++row)
            {
                auto & result = emplace_results.results[row];
                chassert(result.isInserted());

                /// Please note that even when row_size is zero which means the mapped is not OneRow object
                /// but it is still fine to cast mapped to OneRow since the casted OneRow object will only
                /// be accessed with checking row_size
                auto * one_row = reinterpret_cast<OneRow *>(result.getMutableMapped());
                chassert(one_row->empty());
                one_row->resize(value_column_positions.size());
                for (size_t i = 0; auto pos : value_column_positions)
                    input_columns[pos]->get(row, one_row->row[i++]);
            }
        }

        Columns result_chunk_columns;
        result_chunk_columns.reserve(output_chunk_header.getNumColumns());
        for (auto pos : output_column_positions)
            result_chunk_columns.push_back(input_columns[pos]);

        auto delta_column = output_chunk_header.getColumns().back()->cloneEmpty();
        delta_column->insertMany(1, rows);
        result_chunk_columns.push_back(std::move(delta_column));
        output_chunks.emplace_back(std::move(result_chunk_columns), rows);
        return;
    }

    /// Prepare 2 resulting chunks : 1) retracting chunk 2) transformed origin chunk
    auto retract_chunk_columns{output_chunk_header.cloneEmptyColumns()};
    for (auto & col : retract_chunk_columns)
        col->reserve(rows);

    /// In the same chunk, we may have multiple rows which have same primary key values, in this case
    /// we only generate max one retract event.
    /// For example,
    /// 1, k
    /// 2, k
    ///
    /// 1) When `1, k` is processed, if we have seen `k` before, we will retract once and override with `1, k`.
    /// When `2, k` in the same chunk is processed, the latest entry is already `1, k`, in this case, we don't retract again
    /// instead we override `1, k` with `2, k`.
    /// 2) On the other hand, when `1, k` is processed, if we haven't seen it before (brand new key), we will add it as a new key.
    /// when `2, k` in the same chunk is processed, we shall not generate a retract, instead we can just override `1, k` with `2, k`.
    std::unordered_map<const void *, size_t> retracted; /// If same key appears in one chunk, retract once is good enough

    IColumn::Filter filter(rows, 1);
    auto resulting_rows = rows;
    size_t num_retractions = 0;

    for (size_t row = 0; row < rows; ++row)
    {
        /// Please note, when value_column_positions is empty which means there is no value columns cached in
        /// the hybrid hash table, so there is no OneRow record there neither. (SELECT count() FROM mutable_stream).
        /// In the following code, we still cast `mapped` to `OneRow *` no matter value_column_positions is empty or not.
        /// It is still safe to do so even when `mapped` is not pointing to `OneRow` when value_column_positions is empty,
        /// because after casting to `OneRow *`, we will never access it in this case.
        /// In this way, we don't need branching according value_column_positions.empty() which simplifies the code
        /// but indeed introduces lots of subtleties
        auto & emplace_result = emplace_results.results[row];
        auto * mapped = emplace_result.getMutableMapped();
        /// Please note that even when row_size is zero which means the mapped is not OneRow object
        /// but it is still fine to cast mapped to OneRow since the casted OneRow object will only
        /// be accessed with checking row_size
        auto * one_row = reinterpret_cast<OneRow *>(mapped);
        if (emplace_result.isInserted())
        {
            /// This is a new key
            if (!value_column_positions.empty())
            {
                chassert(one_row->row.empty());
                one_row->resize(value_column_positions.size());
                for (size_t i = 0; auto pos : value_column_positions)
                    input_columns[pos]->get(row, one_row->row[i++]);       
            }

            /// No need to generate retraction events when backfilling new keys
            retracted.emplace(mapped, row);
        }
        else
        {
            auto retract_and_update = [&]() {
                /// 1) Retract
                auto iter = retracted.find(mapped);
                if (iter == retracted.end())
                {
                    /// Haven't retract yet, first retract
                    /// Some of the column may be from the key columns and
                    /// some of them are from the row record
                    /// For key columns, we can read from input_columns since they are the same
                    for (auto output_key_col_pos : output_key_column_positions)
                        retract_chunk_columns[output_key_col_pos]->insertFrom(*input_columns[output_key_col_pos], row);

                    /// For value columns, read from row record
                    /// We can use \output_value_column_positions directly, since it's a super set of \value_column_positions
                    /// value_column_positions = output_value_column_positions [+ version_column_positions (if not in output_value_column_positions)]
                    for (size_t i = 0; auto value_column_pos : output_value_column_positions)
                        retract_chunk_columns[value_column_pos]->insert(one_row->row[i++]);

                    ++num_retractions;
                    retracted.emplace(mapped, row);
                }
                else
                {
                    /// Have seen this before, either brand new or retracted
                    filter[iter->second] = 0;
                    iter->second = row;
                    --resulting_rows;
                }

                /// 2) Update then existing row with new values
                for (size_t i = 0; auto pos : value_column_positions)
                    input_columns[pos]->get(row, one_row->row[i++]);
            };

            if (input_version_column_position)
            {
                chassert(version_column_position_in_row);

                const auto & prev_version = one_row->row[*version_column_position_in_row];
                Field current_version;
                input_columns[*input_version_column_position]->get(row, current_version);

                if (prev_version <= current_version)
                {
                    retract_and_update();
                }
                else
                {
                    /// If the current row has smaller version, just drop it on the floor (out of order / late row)
                    /// If they have same version, row arrives at later time is treated having higher version
                    filter[row] = 0;
                    ++late_rows;
                    --resulting_rows;
                }
            }
            else
            {
                retract_and_update();
            }
        }
    }

    /// Final touch for _tp_delta for resulting retract chunk
    if (num_retractions)
    {
        retract_chunk_columns.back()->insertMany(-1, num_retractions);
        output_chunks.emplace_back(std::move(retract_chunk_columns), num_retractions);
        /// Don't stamp watermark on this block since we have a following +1 _tp_delta chunk
        /// updating the old values. If we happen to stamp watermark on it, this will cause
        /// downstream join / aggregation to emit transitive results we don't want.
        /// We also can't concat -1 / +1 chunks since downstream join depends on this separation
        /// behavior meaning depends on a chunk is either all retraction or all append which
        /// largely simplify its logic and usually more performant
        output_chunks.back().setConsecutiveDataFlag();
    }

    /// Composing resulting chunk
    /// If we don't have late rows and we don't have duplicate primary key rows in the same chunk,
    /// we can just reuse the chunk columns (we assume nobody will modify source chunk in-place), otherwise make a copy
    if (resulting_rows == rows)
    {
        /// Fast path
        Columns result_chunk_columns;
        result_chunk_columns.reserve(output_chunk_header.getNumColumns());

        for (auto pos : output_column_positions)
            result_chunk_columns.push_back(input_columns[pos]);

        auto delta_column = output_chunk_header.getColumns().back()->cloneEmpty();
        delta_column->insertMany(1, rows);
        result_chunk_columns.push_back(std::move(delta_column));

        output_chunks.emplace_back(std::move(result_chunk_columns), rows);
    }
    else if (resulting_rows != 0)
    {
        Columns result_chunk_columns;
        result_chunk_columns.reserve(output_chunk_header.getNumColumns());

        /// We will need make a copy of the column data since we only need filter out late rows
        for (auto pos : output_column_positions)
            result_chunk_columns.emplace_back(input_columns[pos]->filter(filter, resulting_rows));

        /// _tp_delta
        auto delta_col = output_chunk_header.getColumns().back()->cloneEmpty();
        delta_col->insertMany(1, resulting_rows);
        result_chunk_columns.emplace_back(std::move(delta_col));

        output_chunks.emplace_back(std::move(result_chunk_columns), resulting_rows);
    }
    else
    {
        output_chunks.push_back(output_chunk_header.clone());
    }
}

Block HybridChangelogConvertTransform::transformOutputHeader(const DB::Block & output_header)
{
    /// Always add _tp_delta at the back of header
    Block transformed_output{output_header};
    if (output_header.has(ProtonConsts::RESERVED_DELTA_FLAG))
    {
        auto pos = output_header.getPositionByName(ProtonConsts::RESERVED_DELTA_FLAG);
        if (pos == output_header.columns() - 1)
            return transformed_output;

        /// remove _tp_delta if not be at back
        transformed_output.erase(pos);
    }

    transformed_output.insert(ColumnWithTypeAndName{std::make_shared<DataTypeInt8>(), ProtonConsts::RESERVED_DELTA_FLAG});
    return transformed_output;
}

void HybridChangelogConvertTransform::transformEmptyChunk()
{
    /// MarkSource is always generating the historical data start / end mark in a separate and empty chunk
    if (!backfill_done)
    {
        if (!backfill_started)
            backfill_started |= input_data.chunk.isHistoricalDataStart();
        else
            backfill_done |= input_data.chunk.isHistoricalDataEnd();
    }

    Columns output_columns;
    output_columns.reserve(output_chunk_header.getNumColumns());

    auto columns = input_data.chunk.detachColumns();
    for (auto pos : output_column_positions)
        output_columns.push_back(std::move(columns[pos]));

    /// _tp_delta column
    output_columns.push_back(output_chunk_header.getColumns().back());
    input_data.chunk.setColumns(std::move(output_columns), 0);

    output_chunks.push_back(std::move(input_data.chunk));
    chassert(!input_data.chunk);
}

void HybridChangelogConvertTransform::createHashTable(const DataTypes & key_column_types, std::string spill_dir, size_t max_hot_key_count)
{
    /// init index hash table
    auto hash_method = chooseHybridHashMethod(key_column_types, /*needs_time_bucket=*/false);
    has_nullable_key = hash_method.has_nullable_key;

    config.spill_dir_path.swap(spill_dir);
    config.max_hot_key_count = max_hot_key_count;

    if (value_column_positions.empty())
    {
        /// If there are no value columns cached in the hybrid table, we install no-op ctor, dtor, serdes
        config.installNoOpCallbacks();
    }
    else
    {
        config.value_object_size = sizeof(OneRow);
        config.align_value_object_size = alignof(OneRow);

        config.value_constructor = [](void * data) { new (data) OneRow; };

        config.value_destructor = [](void * data) {
            auto * row = reinterpret_cast<OneRow *>(data);
            row->~OneRow();
        };

        config.value_serializer = [](const void * data, WriteBuffer & wb) {
            const auto * row = reinterpret_cast<const OneRow *>(data);
            row->serialize(wb);
            return DB::ErrorCodes::OK;
        };

        config.value_deserializer = [](void * data, ReadBuffer & rb) {
            auto * row = reinterpret_cast<OneRow *>(data);
            row->deserialize(rb);
            return DB::ErrorCodes::OK;
        };
    }

    /// Install rocks handler getter
    config.rocks_handler_getter = [this](const std::string & id) {
        return getOrCreateRocks()->getOrCreateHandler(id);
    };

    /// Use another handler of rocks, different from the default \rocks_handler
    index.init(hash_method.type, config.getSubConfig("index"), hash_method.key_sizes, logger);

    key_sizes.swap(hash_method.key_sizes);
}
}
}
