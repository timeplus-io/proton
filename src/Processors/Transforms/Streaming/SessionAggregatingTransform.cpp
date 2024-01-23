#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>

#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Processors/Transforms/Streaming/SessionHelper.h>

#include <ranges>

namespace DB
{
namespace Streaming
{
SessionAggregatingTransform::SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : WindowAggregatingTransform(
        std::move(header),
        std::move(params_),
        std::make_unique<ManyAggregatedData>(1),
        0,
        1,
        1,
        "SessionAggregatingTransform",
        ProcessorID::SessionAggregatingTransformID)
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

    assert(!many_data->hasField());
    many_data->setField(
        {SessionInfoQueue{},
         /// Field serializer
         [](const std::any & field, WriteBuffer & wb, VersionType) { serialize(std::any_cast<const SessionInfoQueue &>(field), wb); },
         /// Field deserializer
         [](std::any & field, ReadBuffer & rb, VersionType) { deserialize(std::any_cast<SessionInfoQueue &>(field), rb); }});
}

std::pair<bool, bool> SessionAggregatingTransform::executeOrMergeColumns(Chunk & chunk, size_t num_rows)
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    auto columns = chunk.detachColumns();
    SessionHelper::assignWindow(
        sessions, window_params, columns, wstart_col_pos, wend_col_pos, time_col_pos, session_start_col_pos, session_end_col_pos);
    num_rows = columns.at(0)->size();
    chunk.setColumns(std::move(columns), num_rows);

    auto result = AggregatingTransform::executeOrMergeColumns(chunk, num_rows);
    if (!sessions.empty())
    {
        if (chunk.hasTimeoutWatermark())
            sessions.back()->active = false; /// force to finalize current session

        auto last_finalized_session = SessionHelper::getLastFinalizedSession(sessions);
        if (last_finalized_session)
        {
            chunk.setWatermark(last_finalized_session->win_end);
            return result;
        }
    }

    if (chunk.hasWatermark())
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

WindowsWithBuckets SessionAggregatingTransform::getLocalWindowsWithBucketsImpl() const
{
    return SessionHelper::getWindowsWithBuckets(many_data->getField<SessionInfoQueue>());
}

void SessionAggregatingTransform::removeBucketsImpl(Int64 /*watermark_*/)
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    Int64 last_expired_time_bucket = SessionHelper::removeExpiredSessions(sessions);
    params->aggregator.removeBucketsBefore(variants, last_expired_time_bucket);
}

}
}
