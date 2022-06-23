#include "SessionAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace
{
    void splitColumns(size_t start, size_t size, IColumn::Filter & filt, size_t late_events, Columns & columns, Columns & to_process)
    {
        assert(columns.size() == to_process.size());
        for (size_t i = 0; i < columns.size(); i++)
        {
            to_process[i] = columns[i]->cut(start, size)->filter(filt, size - late_events);
        }
    }
}

SessionAggregatingTransform::SessionAggregatingTransform(
    Block header, StreamingAggregatingTransformParamsPtr params_)
    : SessionAggregatingTransform(
        std::move(header), std::move(params_), std::make_unique<ManyStreamingAggregatedData>(1), 0, 1, 1)
{
}

SessionAggregatingTransform::SessionAggregatingTransform(
    Block header,
    StreamingAggregatingTransformParamsPtr params_,
    ManyStreamingAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : StreamingAggregatingTransform(std::move(header), std::move(params_), std::move(many_data_), current_variant, max_threads_, temporary_data_merge_threads_, "SessionAggregatingTransform")
{
    assert(params->params.group_by == StreamingAggregator::Params::GroupBy::SESSION);
}

void SessionAggregatingTransform::consume(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    Columns columns = chunk.detachColumns();

    ColumnPtr session_id_column;
    Block merged_block;

    /// Get session_id column
    session_id_column = columns.at(params->params.keys[0]);

    /// Prepare for session window
    ColumnPtr time_column = columns.at(params->params.time_col_pos);

    size_t prev = 0;
    size_t late_events = 0;
    IColumn::Filter filter(num_rows, 1);

    for (size_t i = 0; i < num_rows;)
    {
        SessionStatus status;
        /// filter starts from prev to pos of event to be emitted
        filter.resize_fill(num_rows - prev, 1);

        /// For session window, it has two rounds to execute each row of block if it might trigger a session emit.
        /// the first round is to find all sessions to emit, then emit those sessions before process the current row.
        /// After emit sessions and clear up session info, the second round to process the current row.
        if (params->params.time_col_is_datetime64)
        {
            status = params->aggregator.processSessionRow<ColumnDecimal<DateTime64>>(
                params->aggregator.session_map,
                session_id_column,
                time_column,
                i,
                params->aggregator.max_event_ts);
        }
        else
        {
            status = params->aggregator.processSessionRow<ColumnVector<UInt32>>(
                params->aggregator.session_map,
                session_id_column,
                time_column,
                i,
                params->aggregator.max_event_ts);
        }

        if (status != SessionStatus::IGNORE)
        {
            filter[i - prev] = 1;
        }
        else
        {
            filter[i - prev] = 0;
            late_events++;
        }

        if (status == SessionStatus::EMIT)
        {
            /// if need to emit, first split rows before emit row to process,
            /// then finalizeSessio0 to emit sessions,
            /// last continue to process next row
            Columns to_process(columns.size());
            if (i > prev)
            {
                filter.resize_fill(i - prev);
                splitColumns(prev, i - prev, filter, late_events, columns, to_process);
                /// FIXME: handle the process error when return value is true
                if (!executeOrMergeColumns(to_process))
                    is_consume_finished = false;
                prev = i;
                late_events = 0;
            }

            finalizeSession(params->aggregator.sessions_to_emit, merged_block);
        }
        else
            i++;
    }

    if (prev < num_rows)
    {
        Columns to_process(columns.size());
        splitColumns(prev, num_rows - prev, filter, late_events, columns, to_process);

        /// FIXME: handle the process error when return value is true
        if (!executeOrMergeColumns(to_process))
            is_consume_finished = false;
    }

    if (merged_block.rows() > 0)
    {
        ChunkInfoPtr info = std::make_shared<ChunkInfo>();
        setCurrentChunk(convertToChunk(merged_block), info);
    }
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
/// Only for streaming aggregation case
void SessionAggregatingTransform::finalizeSession(std::vector<size_t> & sessions, Block & merged_block)
{
    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        auto start = MonotonicMilliseconds::now();

        /// FIXME spill to disk, overflow_row etc cases
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));

        if (prepared_data_ptr->empty())
            return;

        /// At least we need one arena in first data item per thread
        StreamingAggregatedDataVariantsPtr & first = prepared_data_ptr->at(0);
        if (max_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < max_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        assert(prepared_data_ptr->at(0)->isTwoLevel());
        mergeTwoLevel(prepared_data_ptr, sessions, merged_block);

        rows_since_last_finalization = 0;

        auto end = MonotonicMilliseconds::now();

        LOG_INFO(
            log, "Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();

        /// We first notify all other variants that the aggregation is done for this round
        /// and then remove the project window buckets and their memory arena for the current variant.
        /// This save a bit time and a bit more efficiency because all variants can do memory arena
        /// recycling in parallel.
        removeBuckets(sessions);
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "StreamingAggregated. Took {} milliseconds to wait for finalizing {} shard aggregation",
            end - start,
            many_data->variants.size());

        removeBuckets(sessions);
    }
}

void SessionAggregatingTransform::mergeTwoLevel(
    ManyStreamingAggregatedDataVariantsPtr & data, const std::vector<size_t> & sessions, Block & final_block)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;
    Block header = first->aggregator->getHeader(true, false).cloneEmpty();
    auto window_start_col = header.getByName(ProtonConsts::STREAMING_WINDOW_START);
    auto window_start_col_ptr = IColumn::mutate(window_start_col.column);
    auto window_end_col = header.getByName(ProtonConsts::STREAMING_WINDOW_END);
    auto window_end_col_ptr = IColumn::mutate(window_end_col.column);

    for (size_t index = data->size() == 1 ? 0 : 1; index < first->aggregates_pools.size(); ++index)
    {
        Arena * arena = first->aggregates_pools.at(index).get();

        /// Figure out which buckets need get merged
        auto & data_variant = data->at(index);

        for (const size_t & session_id : sessions)
        {
            size_t session_rows = 0;

            SessionInfo & info = *(const_cast<SessionHashMap &>(data_variant->aggregator->session_map).getSessionInfo(session_id));
            LOG_DEBUG(log, "emit session {} with {}", info.id, info.toString());

            /// emit session
            std::vector<size_t> buckets = data_variant->aggregator->bucketsOfSession(*data_variant, info.id);

            for (auto bucket : buckets)
            {
                Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket, &is_cancelled);
                if (is_cancelled)
                    return;

                session_rows += block.rows();
                if (merged_block)
                {
                    assertBlocksHaveEqualStructure(merged_block, block, "merging buckets for streaming two level hashtable");
                    for (size_t i = 0, size = merged_block.columns(); i < size; ++i)
                    {
                        const auto source_column = block.getByPosition(i).column;
                        auto mutable_column = IColumn::mutate(std::move(merged_block.getByPosition(i).column));
                        mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                        merged_block.getByPosition(i).column = std::move(mutable_column);
                    }
                }
                else
                    merged_block = std::move(block);
            }

            if (merged_block)
            {
                /// fill session info columns, i.e. 'window_start', 'window_end'
                for (size_t i = 0; i < session_rows; i++)
                {
                    if (params->params.time_col_is_datetime64)
                    {
                        window_start_col_ptr->insert(
                            DecimalUtils::decimalFromComponents<DateTime64>(info.win_start / common::exp10_i64(info.scale), info.win_start % common::exp10_i64(info.scale), info.scale)
                        );
                        window_end_col_ptr->insert(
                            DecimalUtils::decimalFromComponents<DateTime64>(info.win_end / common::exp10_i64(info.scale), info.win_end % common::exp10_i64(info.scale), info.scale)
                        );
                    }
                    else
                    {
                        window_start_col_ptr->insert(info.win_start);
                        window_end_col_ptr->insert(info.win_end);
                    }
                }

                if (params->emit_version)
                    emitVersion(merged_block);
            }
        }
    }
    LOG_DEBUG(log, "total {} sessions, emit {} sessions", params->aggregator.session_map.size(), sessions.size());

    if (merged_block && merged_block.rows() > 0)
    {
        merged_block.insert(1, {std::move(window_end_col_ptr), window_end_col.type, window_end_col.name});
        merged_block.insert(1, {std::move(window_start_col_ptr), window_start_col.type, window_start_col.name});
    }

    if (final_block.rows() > 0)
    {
        assertBlocksHaveEqualStructure(merged_block, final_block, "merging buckets for streaming two level hashtable");
        for (size_t i = 0, size = final_block.columns(); i < size; ++i)
        {
            const auto source_column = merged_block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(final_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            final_block.getByPosition(i).column = std::move(mutable_column);
        }
    }
    else
        final_block = std::move(merged_block);
}

/// Cleanup memory arena for the projected window buckets
void SessionAggregatingTransform::removeBuckets(std::vector<size_t> & sessions)
{
    for (const size_t & session_id : sessions)
        variants.aggregator->removeBucketsOfSession(variants, session_id);

    const_cast<StreamingAggregator *>(variants.aggregator)->clearInfoOfEmitSessions();
}
}
