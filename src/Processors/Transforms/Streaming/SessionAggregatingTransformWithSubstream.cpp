#include <Processors/Transforms/Streaming/SessionAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/Streaming/SessionHelper.h>

namespace DB
{
namespace Streaming
{
SessionAggregatingTransformWithSubstream::SessionAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, size_t current_index)
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
        substream_ctx->setField<SessionInfoQueue>({});
    return substream_ctx;
}

std::pair<bool, bool>
SessionAggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    auto columns = chunk.detachColumns();
    auto & sessions = substream_ctx->getField<SessionInfoQueue>();
    SessionHelper::assignWindow(
        sessions, window_params, columns, wstart_col_pos, wend_col_pos, time_col_pos, session_start_col_pos, session_end_col_pos);

    auto num_rows = columns.at(0)->size();
    chunk.setColumns(std::move(columns), num_rows);

    auto result = AggregatingTransformWithSubstream::executeOrMergeColumns(chunk, substream_ctx);
    if (!sessions.empty())
    {
        for (auto riter = sessions.rbegin(); riter != sessions.rend(); ++riter)
        {
            if (!(*riter)->active)
            {
                chunk.getOrCreateChunkContext()->setWatermark((*riter)->id, (*riter)->id);
                break;
            }
        }
    }
    return result;
}

WindowsWithBucket SessionAggregatingTransformWithSubstream::getFinalizedWindowsWithBucket(
    Int64 watermark, const SubstreamContextPtr & substream_ctx) const
{
    WindowsWithBucket windows_with_bucket;

    auto & sessions = substream_ctx->getField<SessionInfoQueue>();
    for (const auto & session : sessions)
    {
        if (session->id <= watermark)
        {
            assert(!session->active);
            windows_with_bucket.emplace_back(WindowWithBucket{session->win_start, session->win_end, session->id});
        }
    }

    return windows_with_bucket;
}

void SessionAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto & sessions = substream_ctx->getField<SessionInfoQueue>();
    for (auto iter = sessions.begin(); iter != sessions.end();)
    {
        if ((*iter)->id > watermark)
            break;

        iter = sessions.erase(iter);
    }

    params->aggregator.removeBucketsBefore(substream_ctx->variants, watermark);
}

}
}
