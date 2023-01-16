#include "SessionAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{
namespace Streaming
{
SessionAggregatingTransformWithSubstream::SessionAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "SessionAggregatingTransformWithSubstream",
        ProcessorID::SessionAggregatingTransformWithSubstreamID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::SESSION);
}

void SessionAggregatingTransformWithSubstream::consume(Chunk chunk, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    Block merged_block;
    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        /// Get session info
        assert(chunk.hasChunkContext());
        SessionInfo & session_info = substream_ctx->getField<SessionInfo>();

        Columns columns = chunk.detachColumns();

        /// Prepare for session window
        ColumnPtr time_column = columns[params->params.time_col_pos];

        /// FIXME: Better to handle ColumnConst in method `processSessionRow`. The performance should be better.
        ColumnPtr session_start_column = columns[params->params.session_start_pos]->convertToFullColumnIfConst();
        ColumnPtr session_end_column = columns[params->params.session_end_pos]->convertToFullColumnIfConst();

        /// Prepare sessions to emit / process
        std::vector<IColumn::Filter> session_filters;
        SessionInfos sessions_info_to_emit;
        std::tie(session_filters, sessions_info_to_emit)
            = SessionHelper::prepareSession(session_info, time_column, session_start_column, session_end_column, num_rows, params);

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
            if (auto [should_abort, _] = executeOrMergeColumns(std::move(columns_to_process), substream_ctx); should_abort)
                is_consume_finished = true;

            finalizeSession(substream_ctx, sessions_info_to_emit[index_to_emit++], merged_block);
        });

        /// To process session (not emit)
        assert(std::distance(session_end_to_emit, session_columns.end()) <= 1);
        std::for_each(session_end_to_emit, session_columns.end(), [&](auto & columns_to_process) {
            if (auto [should_abort, _] = executeOrMergeColumns(std::move(columns_to_process), substream_ctx); should_abort)
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
void SessionAggregatingTransformWithSubstream::finalizeSession(const SubstreamContextPtr & substream_ctx, const SessionInfo & info, Block & merged_block)
{
    SCOPE_EXIT({ substream_ctx->resetRowCounts(); });

    auto start = MonotonicMilliseconds::now();

    assert(!info.active);
    assert(substream_ctx->getField<SessionInfo>().id == info.id);
    auto & variants = substream_ctx->variants;

    if (variants.empty())
        return;

    assert(!variants.isTwoLevel());
    SessionHelper::finalizeSession(variants, info, merged_block, params);

    if (params->emit_version && params->final)
        emitVersion(merged_block, substream_ctx);

    auto end = MonotonicMilliseconds::now();
    LOG_INFO(log, "Took {} milliseconds to finalize aggregation for session-{}{}", end - start, info.id, info.string());
}

SubstreamContextPtr SessionAggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    auto ctx = AggregatingTransformWithSubstream::getOrCreateSubstreamContext(id);
    if (!ctx->hasField())
    {
        /// Initial session info
        SessionInfo info;
        info.id = id;
        info.scale = params->params.time_scale;
        info.interval = params->params.window_interval;
        info.active = false;

        ctx->setField(std::move(info));
    }
    return ctx;
}

void SessionAggregatingTransformWithSubstream::emitGlobalOversizeSessionsIfPossible(const Chunk & chunk, Block & merged_block)
{
    if (!chunk.hasWatermark())
        return;

    auto max_ts = chunk.getWatermark().watermark;
    if (max_ts >= max_event_ts)
    {
        for (auto & [_, substream_ctx] : substream_contexts)
        {
            auto & info = substream_ctx->getField<SessionInfo>();
            if (!info.active)
                continue;

            /// emit sessions if has oversize session
            if (max_ts > info.max_session_ts)
            {
                LOG_INFO(log, "Found oversize session-{}{}, will finalizing ...", info.id, info.string());
                info.active = false;
                finalizeSession(substream_ctx, info, merged_block);
            }
        }

        max_event_ts = max_ts;
    }
}

}
}
