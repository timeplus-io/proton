#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>

#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/Streaming/SessionWindowHelper.h>
#include <Common/ProtonCommon.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

namespace Streaming
{
SessionAggregatingTransform::SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : WindowAggregatingTransform(
          std::move(header),
          params_,
          std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id),
          0,
          1,
          "SessionAggregatingTransform",
          ProcessorID::SessionAggregatingTransformID)
    , window_params(params->params->window_params->as<SessionWindowParams &>())
{
    if (params->emit_mode == EmitMode::PerEvent)
        throw Exception(ErrorCodes::UNSUPPORTED, "Session window aggregating is not supported for `EMIT PER EVENT`");

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

std::pair<bool, bool> SessionAggregatingTransform::executeColumns(Chunk & chunk, size_t num_rows)
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    auto columns = chunk.detachColumns();
    SessionWindowHelper::assignWindow(
        sessions, window_params, columns, wstart_col_pos, wend_col_pos, time_col_pos, session_start_col_pos, session_end_col_pos);
    num_rows = columns.at(0)->size();
    chunk.setColumns(std::move(columns), num_rows);

    if (num_rows == 0)
        return {false, false};

    auto result = AggregatingTransform::executeColumns(chunk, num_rows);
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
            chunk.setWatermark(SessionWindowHelper::getWatermarkForPeriodicEmit(sessions, chunk.getWatermark()));
        }
    }

    return result;
}

WindowsWithBuckets SessionAggregatingTransform::getLocalWindowsWithBucketsImpl() const
{
    return SessionWindowHelper::getWindowsWithBuckets(many_data->getField<SessionInfoQueue>());
}

void SessionAggregatingTransform::removeBucketsImpl(Int64 watermark_)
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    Int64 last_expired_time_bucket = SessionWindowHelper::removeExpiredSessions(sessions, watermark_);
    return params->aggregator->removeBucketsBefore(variants, last_expired_time_bucket, transform_id);
}

String SessionAggregatingTransform::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "SessionAggregatingTransform";
        case AggregatorType::Hybrid:
            return "HybridSessionAggregatingTransform";
    }
}
}

}
