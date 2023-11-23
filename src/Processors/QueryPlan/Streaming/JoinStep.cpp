#include <Processors/QueryPlan/Streaming/JoinStep.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/Streaming/IHashJoin.h>
#include <Processors/Transforms/Streaming/JoinTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    size_t join_max_cached_bytes_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , max_streams(max_streams_)
    , join_max_cached_bytes(join_max_cached_bytes_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream{
        .header = JoinTransform::transformHeader(left_stream_.header, std::dynamic_pointer_cast<IHashJoin>(join)),
    };

    for (const auto & input_stream : input_streams)
        output_stream->is_streaming |= input_stream.is_streaming;
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamingJoinStep expect two input steps");

    return QueryPipelineBuilder::joinPipelinesStreaming(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        output_stream->header,
        max_block_size,
        max_streams,
        join_max_cached_bytes,
        &processors);
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}
}
}
