#include "SessionAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{
namespace Streaming
{
SessionAggregatingTransform::SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransform(std::move(header), std::move(params_), "SessionAggregatingTransform", ProcessorID::SessionAggregatingTransformID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::SESSION);

    /// Initial session info
    session_info.id = SessionID{};
    session_info.scale = params->params.time_scale;
    session_info.interval = params->params.window_interval;
    session_info.active = false;
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
            auto process_rows = columns_to_process[0]->size();
            if (auto [should_abort, _] = executeOrMergeColumns(std::move(columns_to_process), process_rows); should_abort)
                is_consume_finished = true;

            finalizeSession(sessions_info_to_emit[index_to_emit++], merged_block);
        });

        /// To process session (not emit)
        assert(std::distance(session_end_to_emit, session_columns.end()) <= 1);
        std::for_each(session_end_to_emit, session_columns.end(), [&](auto & columns_to_process) {
            auto process_rows = columns_to_process[0]->size();
            if (auto [should_abort, _] = executeOrMergeColumns(std::move(columns_to_process), process_rows); should_abort)
                is_consume_finished = true;
        });
    }

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

    assert(!info.active);

    if (variants.empty())
        return;

    assert(!variants.isTwoLevel());
    SessionHelper::finalizeSession(variants, info, merged_block, params);

    if (params->emit_version && params->final)
        emitVersion(merged_block);

    auto end = MonotonicMilliseconds::now();
    LOG_INFO(log, "Took {} milliseconds to finalize aggregation for session-{}{}", end - start, info.id, info.string());
}

}
}
