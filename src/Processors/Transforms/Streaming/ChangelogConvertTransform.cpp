#include <Processors/Transforms/Streaming/ChangelogConvertTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/ChooseHashMethod.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_IDENTIFIER;
}

namespace Streaming
{

ChangelogConvertTransform::ChangelogConvertTransform(
    const DB::Block & input_header,
    const DB::Block & output_header,
    std::vector<std::string> key_column_names,
    const std::string & version_column_name)
    : IProcessor({input_header}, {output_header}, ProcessorID::ChangelogConvertTransformID)
    , output_chunk_header(outputs.front().getHeader().getColumns(), 0)
    , source_chunks(cached_block_metrics)
    , last_log_ts(MonotonicMilliseconds::now())
    , logger(&Poco::Logger::get("ChangelogConvertTransform"))
{
    assert(!key_column_names.empty());
    assert(!input_header.has(ProtonConsts::RESERVED_DELTA_FLAG));
    assert(output_header.getByPosition(output_header.columns() - 1).name == ProtonConsts::RESERVED_DELTA_FLAG);

    output_column_positions.reserve(output_header.columns());

    for (const auto & col : output_header)
        if (col.name != ProtonConsts::RESERVED_DELTA_FLAG)
            output_column_positions.push_back(input_header.getPositionByName(col.name));

    assert(output_column_positions.size() == output_header.columns() - 1);

    ColumnRawPtrs key_columns;
    key_columns.reserve(key_column_names.size());
    key_column_positions.reserve(key_column_names.size());

    for (const auto & key_col : key_column_names)
    {
        key_column_positions.push_back(input_header.getPositionByName(key_col));
        key_columns.push_back(input_header.getByPosition(key_column_positions.back()).column.get());
    }

    if (!version_column_name.empty())
        version_column_position = input_header.getPositionByName(version_column_name);

    /// init index hash table
    auto [hash_type, key_sizes_] = chooseHashMethod(key_columns);
    index.create(hash_type);
    key_sizes.swap(key_sizes_);

    auto key_sizes_str_v = key_sizes | std::views::transform([](const auto & size) { return std::to_string(size); });
    LOG_INFO(
        logger,
        "Prepare converting changelog by keys_num={}, keys_size={} (each key size: {}), input_header={}",
        key_sizes.size(),
        std::accumulate(key_sizes.begin(), key_sizes.end(), static_cast<size_t>(0)),
        fmt::join(key_sizes_str_v, ", "),
        input_header.dumpStructure());
}

IProcessor::Status ChangelogConvertTransform::prepare()
{
    /// std::scoped_lock lock(mutex);
    /// FIXME, multiple shards / inputs

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
        assert(!input_data.chunk);

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

void ChangelogConvertTransform::work()
{
    if (input_data.exception)
        std::rethrow_exception(input_data.exception);

    const auto & chunk = input_data.chunk;
    if (auto ckpt_ctx = chunk.getCheckpointContext(); ckpt_ctx)
    {
        assert(chunk.rows() == 0);
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
            materialized_columns.push_back(columns[key_col_pos]->convertToFullColumnIfConst());
            key_columns.push_back(materialized_columns.back().get());
        }

        switch (index.type)
        {
#define M(TYPE) \
    case HashType::TYPE: \
        retractAndIndex<typename KeyGetterForType<HashType::TYPE, std::remove_reference_t<decltype(*index.TYPE)>>::Type>( \
            rows, key_columns, *index.TYPE); \
        break;
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }
    }
    else
        transformEmptyChunk();

    /// Every 30 seconds, log metrics
    if (MonotonicMilliseconds::now() - last_log_ts > 30'000)
    {
        size_t hash_total_row_count = index.getTotalRowCount();
        size_t hash_total_row_refs_bytes = hash_total_row_count * sizeof(RefCountDataBlock<LightChunk>);
        size_t hash_buffer_bytes = index.getBufferSizeInBytes();
        size_t hash_buffer_cells = index.getBufferSizeInCells();

        LOG_INFO(
            logger,
            "Cached source blocks metrics: {}; hash table metrics: hash_total_rows={} hash_total_bytes={} (total_bytes_in_arena:{} "
            "hash_row_refs_bytes:{} hash_buffer_bytes:{} hash_buffer_cells:{}); late_rows={}; earliest_cached_block_ref_count={};",
            cached_block_metrics.string(),
            hash_total_row_count,
            hash_buffer_bytes + hash_total_row_refs_bytes + pool.size(),
            pool.size(),
            hash_total_row_refs_bytes,
            hash_buffer_bytes,
            hash_buffer_cells,
            late_rows,
            source_chunks.empty() ? 0 : source_chunks.begin()->refCount());

        last_log_ts = MonotonicMilliseconds::now();
    }
}

template <typename KeyGetter, typename Map>
void ChangelogConvertTransform::retractAndIndex(size_t rows, const ColumnRawPtrs & key_columns, Map & map)
{
    /// std::scoped_lock lock(mutex);

    source_chunks.pushBack(std::move(input_data.chunk));
    assert(!input_data.chunk);

    /// Prepare 2 resulting chunks : 1) retracting chunk 2) transformed origin chunk
    auto retract_chunk_columns{output_chunk_header.cloneEmptyColumns()};
    for (auto & col : retract_chunk_columns)
        col->reserve(rows);

    KeyGetter key_getter(key_columns, key_sizes, nullptr);

    size_t num_retractions = 0;
    const auto & last_inserted_chunk = source_chunks.lastDataBlock();
    const auto & last_inserted_chunk_columns = last_inserted_chunk.getColumns();

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

    std::unordered_map<typename Map::mapped_type::element_type *, size_t> retracted;

    IColumn::Filter filter(rows, 1);

    Arena lookup_pool;

    for (size_t row = 0; row < rows; ++row)
    {
        auto find_result = key_getter.findKey(map, row, lookup_pool);
        if (find_result.isFound())
        {
            /// This is an existing key
            auto & mapped = find_result.getMapped();

            auto retract_and_update = [&]() {
                /// 1) Retract
                auto iter = retracted.find(mapped.get());
                if (iter == retracted.end())
                {
                    /// Never saw this, first retract
                    const auto & prev_source_chunk_columns = mapped->block().getColumns();
                    for (size_t i = 0; auto pos : output_column_positions)
                        retract_chunk_columns[i++]->insertFrom(*prev_source_chunk_columns[pos], mapped->row_num);

                    ++num_retractions;
                    retracted.emplace(mapped.get(), row);
                }
                else
                {
                    /// Have seen this before, either brand new or retracted
                    filter[iter->second] = 0;
                    iter->second = row;
                }

                /// 2) Override existing ref which will deref previous reference
                *mapped = typename Map::mapped_type::element_type(&source_chunks, row);
            };

            if (version_column_position)
            {
                const auto & prev_version_column = mapped->block().getColumns()[*version_column_position];
                const auto & current_version_column = last_inserted_chunk_columns[*version_column_position];
                /// Check `version` column
                /// If the current row has smaller version, just drop it on the floor (out of order / late row)
                /// If they have same version, row arrives at later time is treated having higher version
                if (current_version_column->compareAt(row, mapped->row_num, *prev_version_column, -1) >= 0)
                {
                    retract_and_update();
                }
                else
                {
                    filter[row] = 0;
                    ++late_rows;
                }
            }
            else
                retract_and_update();
        }
        else
        {
            /// This is a new key
            auto emplace_result = key_getter.emplaceKey(map, row, pool);
            assert(emplace_result.isInserted());

            auto & mapped = emplace_result.getMapped();
            new (&mapped) typename Map::mapped_type(std::make_unique<typename Map::mapped_type::element_type>(&source_chunks, row));

            /// Brand new key is treated as retracted since there is nothing to retract
            retracted.emplace(mapped.get(), row);
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

    size_t resulting_rows = 0;
    for (auto v : filter)
        resulting_rows += v;

    if (resulting_rows == rows)
    {
        /// Fast path
        Columns result_chunk_columns;
        result_chunk_columns.reserve(output_chunk_header.getNumColumns());

        for (auto pos : output_column_positions)
            result_chunk_columns.push_back(last_inserted_chunk_columns[pos]);

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
            result_chunk_columns.emplace_back(last_inserted_chunk_columns[pos]->filter(filter, resulting_rows));

        /// _tp_delta
        auto delta_col = output_chunk_header.getColumns().back()->cloneEmpty();
        delta_col->insertMany(1, resulting_rows);
        result_chunk_columns.emplace_back(std::move(delta_col));

        output_chunks.emplace_back(std::move(result_chunk_columns), resulting_rows);
    }
    else
        output_chunks.push_back(output_chunk_header.clone());
}

Block ChangelogConvertTransform::transformOutputHeader(const DB::Block & output_header)
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

void ChangelogConvertTransform::transformEmptyChunk()
{
    Columns output_columns;
    output_columns.reserve(output_chunk_header.getNumColumns());

    auto columns = input_data.chunk.detachColumns();
    for (auto pos : output_column_positions)
        output_columns.push_back(std::move(columns[pos]));

    /// _tp_delta column
    output_columns.push_back(output_chunk_header.getColumns().back());
    input_data.chunk.setColumns(std::move(output_columns), 0);

    output_chunks.push_back(std::move(input_data.chunk));
    assert(!input_data.chunk);
}


void ChangelogConvertTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        SerializedBlocksToIndices serialized_blocks_to_indices;
        source_chunks.serialize(wb, getVersion(), getInputs().front().getHeader(), &serialized_blocks_to_indices);

        index.serialize(
            /*MappedSerializer*/
            [&](const std::unique_ptr<RowRefWithRefCount<LightChunk>> & mapped_, WriteBuffer & wb_) {
                assert(mapped_);
                mapped_->serialize(serialized_blocks_to_indices, wb_);
            },
            wb);

        DB::writeIntBinary(late_rows, wb);

        if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
            cached_block_metrics.serialize(wb, getVersion());
    });
}

void ChangelogConvertTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType version_, ReadBuffer & rb) {
        DeserializedIndicesToBlocks<LightChunk> deserialized_indices_to_blocks;
        source_chunks.deserialize(rb, version_, getInputs().front().getHeader(), &deserialized_indices_to_blocks);

        index.deserialize(
            /*MappedDeserializer*/
            [&](std::unique_ptr<RowRefWithRefCount<LightChunk>> & mapped_, Arena &, ReadBuffer & rb_) {
                mapped_ = std::make_unique<RowRefWithRefCount<LightChunk>>();
                mapped_->deserialize(&source_chunks, deserialized_indices_to_blocks, rb_);
            },
            pool,
            rb);

        DB::readIntBinary(late_rows, rb);

        if (version_ <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
            cached_block_metrics.deserialize(rb, version_);
    });
}
}
}