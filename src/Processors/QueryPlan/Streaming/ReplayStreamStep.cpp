#include <Processors/QueryPlan/Streaming/ReplayStreamStep.h>
#include <Processors/Streaming/ReplayStreamTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/ProtonCommon.h>


namespace DB
{
namespace Streaming
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

ReplayStreamStep::ReplayStreamStep(
    const DataStream & input_stream_, Float32 replay_speed_, const String & replay_time_col_, std::vector<Int64> shards_last_sns_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , replay_speed(replay_speed_)
    , shards_last_sns(std::move(shards_last_sns_))
    , replay_time_col(replay_time_col_)
{
}

void ReplayStreamStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// FIXME: `StorageStream::getLastSNs()` result is invalid for multiple shards in cluster
    if (unlikely(pipeline.getNumStreams() != shards_last_sns.size()))
        throw Exception("Input Stream is not equal to the shard's num", ErrorCodes::LOGICAL_ERROR);

    size_t index = 0;
    pipeline.addSimpleTransform([&](const Block & header) {
        return std::make_shared<ReplayStreamTransform>(header, replay_speed, shards_last_sns[index++], replay_time_col);
    });
}

}
}
