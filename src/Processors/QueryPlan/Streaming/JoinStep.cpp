#include "JoinStep.h"

#include <Interpreters/IJoin.h>
#include <Interpreters/Streaming/HashJoin.h>
#include <Processors/Transforms/Streaming/JoinTranform.h>
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
    UInt64 join_max_cached_bytes_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , join_max_cached_bytes(join_max_cached_bytes_)
{
    auto hash_join = std::dynamic_pointer_cast<HashJoin>(join);
    /// We know the finalized left header
    hash_join->initLeftStream(left_stream_.header);

    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream{
        .header = JoinTransform::transformHeader(left_stream_.header, hash_join),
    };
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamingJoinStep expect two input steps");

    return QueryPipelineBuilder::joinPipelinesStreaming(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        max_block_size,
        join_max_cached_bytes,
        &processors);
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}
}
}
