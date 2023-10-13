#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>

#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Processors/Transforms/Streaming/SessionHelper.h>

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
         [](const std::any & field, WriteBuffer & wb) { serialize(std::any_cast<const SessionInfoQueue &>(field), wb); },
         /// Field deserializer
         [](std::any & field, ReadBuffer & rb) { deserialize(std::any_cast<SessionInfoQueue &>(field), rb); }});
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

        for (auto riter = sessions.rbegin(); riter != sessions.rend(); ++riter)
        {
            if (!(*riter)->active)
            {
                chunk.getOrCreateChunkContext()->setWatermark((*riter)->id);
                return result;
            }
        }
    }

    chunk.clearWatermark();

    return result;
}

WindowsWithBuckets SessionAggregatingTransform::getLocalFinalizedWindowsWithBucketsImpl(Int64 watermark_) const
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    WindowsWithBuckets windows_with_buckets;
    for (const auto & session : sessions)
    {
        if (session->id <= watermark_)
        {
            assert(!session->active || watermark_ == TIMEOUT_WATERMARK);
            windows_with_buckets.emplace_back(WindowWithBuckets{{session->win_start, session->win_end}, {session->id}});
        }
    }

    return windows_with_buckets;
}

void SessionAggregatingTransform::removeBucketsImpl(Int64 watermark_)
{
    auto & sessions = many_data->getField<SessionInfoQueue>();
    for (auto iter = sessions.begin(); iter != sessions.end();)
    {
        if ((*iter)->id > watermark_)
            break;

        iter = sessions.erase(iter);
    }

    params->aggregator.removeBucketsBefore(variants, watermark_);
}

}
}
