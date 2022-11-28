#include "SessionAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{

SessionAggregatingTransform::SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : SessionAggregatingTransform(std::move(header), std::move(params_), std::make_shared<SessionManyAggregatedData>(1), 0, 1, 1)
{
}

SessionAggregatingTransform::SessionAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    SessionManyAggregatedDataPtr session_many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        session_many_data_->substream_many_data,
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "SessionAggregatingTransform",
        ProcessorID::SessionAggregatingTransformID)
    , session_many_data(std::move(session_many_data_))
    , session_map(*session_many_data->sessions_maps[current_variant])
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::SESSION);

    /// FIXME: support parallel aggregating
    if (many_aggregating_size != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel processing session window is not supported");
}

void SessionAggregatingTransform::consume(Chunk chunk)
{
    Block merged_block;
    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        Columns columns = chunk.detachColumns();

        /// Prepare for session window
        ColumnPtr time_column = columns[params->params.time_col_pos];

        /// FIXME: Better to handle ColumnConst in method `processSessionRow`. The performance should be better.
        ColumnPtr session_start_column = columns[params->params.session_start_pos]->convertToFullColumnIfConst();
        ColumnPtr session_end_column = columns[params->params.session_end_pos]->convertToFullColumnIfConst();

        /// Get session info
        assert(chunk.hasChunkContext());
        SessionInfo & session_info = getOrCreateSessionInfo(chunk.getSubstreamID());

        /// Prepare sessions to emit / process
        std::vector<IColumn::Filter> session_filters;
        SessionInfos sessions_info_to_emit;
        if (params->params.time_col_is_datetime64)
            std::tie(session_filters, sessions_info_to_emit)
                = prepareSession<ColumnDecimal<DateTime64>>(session_info, time_column, session_start_column, session_end_column, num_rows);
        else
            std::tie(session_filters, sessions_info_to_emit)
                = prepareSession<ColumnVector<UInt32>>(session_info, time_column, session_start_column, session_end_column, num_rows);

        /// Prepare data in sessions
        /// [To process and emit session(s) ...] + [To process session]
        auto session_count = session_filters.size();
        std::vector<Columns> session_columns(session_count, Columns(columns.size()));
        for (size_t session_idx = 0; session_idx < session_count; ++session_idx)
        {
            const auto & filter = session_filters[session_idx];
            auto & session_cols = session_columns[session_idx];
            for (size_t col_idx = 0; auto & column : columns)
            {
                session_cols[col_idx] = column->filter(filter, -1);
                ++col_idx;
            }
        }

        /// To process and emit session(s)
        auto session_begin_to_emit = session_columns.begin();
        auto session_end_to_emit = std::next(session_begin_to_emit, sessions_info_to_emit.size());
        size_t index_to_emit = 0;
        std::for_each(session_begin_to_emit, session_end_to_emit, [&](auto & columns_to_process) {
            if (!executeOrMergeColumns(std::move(columns_to_process), session_info.id))
                is_consume_finished = true;

            finalizeSession(sessions_info_to_emit[index_to_emit++], merged_block);
        });

        /// To process session (not emit)
        assert(std::distance(session_end_to_emit, session_columns.end()) <= 1);
        std::for_each(session_end_to_emit, session_columns.end(), [&](auto & columns_to_process) {
            if (!executeOrMergeColumns(std::move(columns_to_process), session_info.id))
                is_consume_finished = true;
        });
    }

    /// Try emit oversize sessions
    emitGlobalOversizeSessionsIfPossible(chunk, merged_block);

    if (merged_block.rows() > 0)
        setCurrentChunk(convertToChunk(merged_block), nullptr);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
/// Only for streaming aggregation case
void SessionAggregatingTransform::finalizeSession(const SessionInfo & info, Block & merged_block)
{
    SCOPE_EXIT({ many_data->resetRowCounts(); });

    auto start = MonotonicMilliseconds::now();

    auto substream_ctx = getOrCreateSubstreamContext(info.id);

    /// We lock all variants of current substream to merge
    {
        std::lock_guard lock(substream_ctx->variants_mutex);

        auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(substream_ctx->many_variants);
        if (prepared_data_ptr->empty())
            return;

        AggregatedDataVariantsPtr & first = prepared_data_ptr->at(0);
        /// At least we need one arena in first data item per thread
        Arenas & first_pool = first->aggregates_pools;
        for (size_t j = first_pool.size(); j < max_threads; j++)
            first_pool.emplace_back(std::make_shared<Arena>());

        assert(!prepared_data_ptr->at(0)->isTwoLevel());
        convertSingleLevel(prepared_data_ptr, info, merged_block);

        /// Clear and reuse current session variants
        for (auto & variants : substream_ctx->many_variants)
        {
            variants->init(variants->type);
            variants->aggregates_pools = Arenas(1, std::make_shared<Arena>());
            variants->aggregates_pool = variants->aggregates_pools.back().get();
            params->aggregator.initStatesForWithoutKeyOrOverflow(*variants);
        }
    }

    auto end = MonotonicMilliseconds::now();
    LOG_INFO(
        log,
        "Took {} milliseconds to finalize {} shard aggregation for session-{}{}",
        end - start,
        many_aggregating_size,
        info.id,
        info.string());
}

void SessionAggregatingTransform::convertSingleLevel(ManyAggregatedDataVariantsPtr & data, const SessionInfo & info, Block & final_block)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    assert(data->size() == 1);
    auto & first = data->at(0);

    Block header = params->aggregator.getHeader(true, false).cloneEmpty();
    auto window_start_col = header.getByName(ProtonConsts::STREAMING_WINDOW_START);
    auto window_start_col_ptr = IColumn::mutate(window_start_col.column);
    auto window_end_col = header.getByName(ProtonConsts::STREAMING_WINDOW_END);
    auto window_end_col_ptr = IColumn::mutate(window_end_col.column);

    Block merged_block;
    if (first->without_key)
        merged_block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != AggregatedDataVariants::Type::without_key, ConvertAction::STREAMING_EMIT);
    else
        merged_block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final, ConvertAction::STREAMING_EMIT);

    /// NOTE: In case no aggregate key and no aggregate function, the merged_block shall be empty.
    /// e.g. `select window_start, window_end, date_diff('second', window_start, window_end) from session(test_session, 5s, [location='ca', location='sh')) group by window_start, window_end`
    size_t session_rows = merged_block ? merged_block.rows() : 1;
    assert(session_rows > 0);
    for (size_t i = 0; i < session_rows; i++)
    {
        if (params->params.time_col_is_datetime64)
        {
            window_start_col_ptr->insert(DecimalUtils::decimalFromComponents<DateTime64>(
                info.win_start / common::exp10_i64(info.scale), info.win_start % common::exp10_i64(info.scale), info.scale));
            window_end_col_ptr->insert(DecimalUtils::decimalFromComponents<DateTime64>(
                info.win_end / common::exp10_i64(info.scale), info.win_end % common::exp10_i64(info.scale), info.scale));
        }
        else
        {
            window_start_col_ptr->insert(info.win_start);
            window_end_col_ptr->insert(info.win_end);
        }
    }

    /// fill session info columns, i.e. 'window_start', 'window_end'
    /// FIXME, this looks pretty hacky as we need align the column positions / header here with Aggregator::Params::getHeader(...)
    merged_block.insert(0, {std::move(window_end_col_ptr), window_end_col.type, window_end_col.name});
    merged_block.insert(0, {std::move(window_start_col_ptr), window_start_col.type, window_start_col.name});

    if (params->emit_version)
        emitVersion(merged_block, info.id);

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

SessionInfo & SessionAggregatingTransform::getOrCreateSessionInfo(const SessionID & id)
{
    auto & session_info_ptr = session_map[id];
    if (!session_info_ptr)
    {
        /// Initial session info
        session_info_ptr = std::make_unique<SessionInfo>();
        session_info_ptr->id = id;
        session_info_ptr->scale = params->params.time_scale;
        session_info_ptr->interval = params->params.window_interval;
        session_info_ptr->active = false;
    }
    return *session_info_ptr;
}

SessionInfo & SessionAggregatingTransform::getSessionInfo(const SessionID & session_id)
{
    auto iter = session_map.find(session_id);
    if (iter == session_map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found session info: '{}'", session_id);

    return *(iter->second);
}

template <typename TargetColumnType>
std::pair<std::vector<IColumn::Filter>, SessionInfos> SessionAggregatingTransform::prepareSession(
    SessionInfo & info, ColumnPtr & time_column, ColumnPtr & session_start_column, ColumnPtr & session_end_column, size_t num_rows)
{
    Block block;
    const typename TargetColumnType::Container & time_vec = checkAndGetColumn<TargetColumnType>(time_column.get())->getData();

    /// session_start_column could be a ColumnConst object
    const typename ColumnBool::Container & session_start_vec = checkAndGetColumn<ColumnBool>(session_start_column.get())->getData();
    const typename ColumnBool::Container & session_end_vec = checkAndGetColumn<ColumnBool>(session_end_column.get())->getData();

    IColumn::Filter filter(num_rows, 0);
    SessionInfos sessions_info_to_emit;
    std::vector<IColumn::Filter> session_filters;
    size_t offset = 0;
    Int64 ts_secs = time_vec[0];
    Bool session_start = session_start_vec[0];
    Bool session_end = session_end_vec[0];

    const DateLUTImpl & time_zone = DateLUT::instance("UTC");

    auto ignore_row = [&]() { filter[offset] = 0; };

    auto keep_row = [&]() {
        filter[offset] = 1;
        /// Extend session window end and timeout timestamp
        if (ts_secs > info.win_end)
        {
            info.win_end = ts_secs;
            info.timeout_ts = addTime(info.win_end, params->params.interval_kind, params->params.window_interval, time_zone, info.scale);
        }
    };

    /// Keep/ignore the start session row
    auto keep_row_if_include_start = [&]() {
        if (params->params.window_desc->start_with_boundary)
            keep_row();
        else
            ignore_row();
    };

    /// Keep/ignore the close session row
    auto keep_row_if_include_end = [&]() {
        if (params->params.window_desc->end_with_boundary)
            keep_row();
        else
        {
            ignore_row();
            /// Not keep but still need to extend the window end
            if (ts_secs > info.win_end)
                info.win_end = ts_secs;
        }
    };

    auto open_session = [&]() {
        info.win_start = ts_secs;
        info.win_end = ts_secs + 1;
        info.timeout_ts = addTime(info.win_end, params->params.interval_kind, params->params.window_interval, time_zone, info.scale);
        info.ignore_ts = addTime(info.win_start, params->params.timeout_kind, -1 * params->params.session_size, time_zone, info.scale);
        info.max_session_ts = addTime(info.win_start, params->params.timeout_kind, params->params.session_size, time_zone, info.scale);
        info.active = true;
    };

    auto close_session = [&]() {
        info.active = false;
        sessions_info_to_emit.emplace_back(info);
        session_filters.emplace_back(num_rows, 0);
        session_filters.back().swap(filter);
    };

    for (; offset < num_rows; ++offset)
    {
        ts_secs = time_vec[offset];
        session_start = session_start_vec[offset];
        session_end = session_end_vec[offset];

        if (info.active)
        {
            /// Special handlings:
            /// 1) if event_time < @ingore_ts, ingore event
            /// 2) if event_time > @timeout_ts, close session
            /// 3) if event_time > @max_session_ts, close session
            /// 4) if session end condition is true, close session
            assert(info.win_start <= info.win_end);
            if (ts_secs < info.ignore_ts)
                ignore_row();
            else if (ts_secs > info.timeout_ts || ts_secs > info.max_session_ts)
            {
                /// Timeout not keep current row.
                close_session();

                if (session_start)
                {
                    open_session();
                    keep_row_if_include_start();
                }
            }
            else if (session_end)
            {
                keep_row_if_include_end();
                close_session();

                if (session_start)
                {
                    open_session();
                    keep_row_if_include_start();
                }
            }
            else
                keep_row();
        }
        else
        {
            /// Active session window
            if (session_start)
            {
                open_session();
                keep_row_if_include_start();
            }
            else
                ignore_row();
        }
    }

    if (info.active)
        session_filters.emplace_back(std::move(filter));

    return {std::move(session_filters), std::move(sessions_info_to_emit)};
}

void SessionAggregatingTransform::emitGlobalOversizeSessionsIfPossible(const Chunk & chunk, Block & merged_block)
{
    if (!chunk.hasWatermark())
        return;

    auto max_ts = chunk.getWatermark().watermark;
    if (max_ts >= max_event_ts)
    {
        for (auto & [_, session_info] : session_map)
        {
            if (!session_info->active)
                continue;

            /// emit sessions if has oversize session
            if (max_ts > session_info->max_session_ts)
            {
                LOG_INFO(log, "Found oversize session-{}{}, will finalizing ...", session_info->id, session_info->string());
                session_info->active = false;
                finalizeSession(*session_info, merged_block);
            }
        }

        max_event_ts = max_ts;
    }
}

}
}
