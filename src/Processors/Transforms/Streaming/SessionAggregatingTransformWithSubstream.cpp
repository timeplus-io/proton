#include <Processors/Transforms/Streaming/SessionAggregatingTransformWithSubstream.h>

#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/Streaming/SessionWindowHelper.h>

namespace DB
{
namespace Streaming
{
SessionAggregatingTransformWithSubstream::SessionAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : WindowAggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "SessionAggregatingTransformWithSubstream",
        ProcessorID::SessionAggregatingTransformWithSubstreamID)
    , window_params(params->params.window_params->as<SessionWindowParams &>())
{
    const auto & input_header = getInputs().front().getHeader();
    if (input_header.has(ProtonConsts::STREAMING_WINDOW_START))
        wstart_col_pos = input_header.getPositionByName(ProtonConsts::STREAMING_WINDOW_START);

    if (input_header.has(ProtonConsts::STREAMING_WINDOW_END))
        wend_col_pos = input_header.getPositionByName(ProtonConsts::STREAMING_WINDOW_END);

    time_col_pos = input_header.getPositionByName(window_params.desc->argument_names[0]);
    session_start_col_pos = input_header.getPositionByName(ProtonConsts::STREAMING_SESSION_START);
    session_end_col_pos = input_header.getPositionByName(ProtonConsts::STREAMING_SESSION_END);
}

SubstreamContextPtr SessionAggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    auto substream_ctx = AggregatingTransformWithSubstream::getOrCreateSubstreamContext(id);
    if (!substream_ctx->hasField())
        substream_ctx->setField(
            {SessionInfoQueue{},
             /// Field serializer
             [](const std::any & field, WriteBuffer & wb, VersionType) { serialize(std::any_cast<const SessionInfoQueue &>(field), wb); },
             /// Field deserializer
             [](std::any & field, ReadBuffer & rb, VersionType) { deserialize(std::any_cast<SessionInfoQueue &>(field), rb); }});
    return substream_ctx;
}

std::pair<bool, bool>
SessionAggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    auto columns = chunk.detachColumns();
    auto & sessions = substream_ctx->getField<SessionInfoQueue>();
    /// window start/end column are replaced with session id
    SessionWindowHelper::assignWindow(
        sessions, window_params, columns, wstart_col_pos, wend_col_pos, time_col_pos, session_start_col_pos, session_end_col_pos);

    auto num_rows = columns.at(0)->size();
    chunk.setColumns(std::move(columns), num_rows);

    auto result = AggregatingTransformWithSubstream::executeOrMergeColumns(chunk, substream_ctx);
    if (!sessions.empty())
    {
        if (chunk.hasTimeoutWatermark())
            sessions.back()->active = false; /// force to finalize current session

        auto last_finalized_session = SessionWindowHelper::getLastFinalizedSession(sessions);
        if (last_finalized_session)
        {
            chunk.setWatermark(last_finalized_session->win_end);
            return result;
        }
    }

    /// No finalized sessions
    if (AggregatingHelper::onlyEmitFinalizedWindows(params->emit_mode))
    {
        chunk.clearWatermark();
    }
    else if (chunk.hasWatermark())
    {
        /// When get here, there are two scenarios:
        /// 1) No sessions, and have periodic watermark or timeout watermark
        /// 2) Only has one active session, and have periodic watermark
        /// For case 2), we can set session start as watermark to just emit current session (for periodic emit)
        if (!sessions.empty())
        {
            assert(sessions.size() == 1 && sessions.front()->active);
            chunk.setWatermark(sessions.front()->win_start);
        }
    }

    return result;
}

WindowsWithBuckets SessionAggregatingTransformWithSubstream::getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const
{
    return SessionWindowHelper::getWindowsWithBuckets(substream_ctx->getField<SessionInfoQueue>());
}

Window SessionAggregatingTransformWithSubstream::getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const
{
    /// The finalized sessions already are removed, so we don't care it.
    return {INVALID_WATERMARK, INVALID_WATERMARK};
}

void SessionAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto & sessions = substream_ctx->getField<SessionInfoQueue>();
    Int64 last_expired_time_bucket = SessionWindowHelper::removeExpiredSessions(sessions, watermark);
    params->aggregator.removeBucketsBefore(substream_ctx->variants, last_expired_time_bucket);
}

}
}
