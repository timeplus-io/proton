#include "StreamingJoinStep.h"

#include <Interpreters/IJoin.h>
#include <Interpreters/Streaming/StreamingHashJoin.h>
#include <Processors/Transforms/Streaming/StreamingJoinTranform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StreamingJoinStep::StreamingJoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_wait_ms_,
    UInt64 join_max_wait_rows_,
    UInt64 join_max_cached_bytes_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , join_max_wait_ms(join_max_wait_ms_)
    , join_max_wait_rows(join_max_wait_rows_)
    , join_max_cached_bytes(join_max_cached_bytes_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream{
        .header = StreamingJoinTransform::transformHeader(left_stream_.header, std::dynamic_pointer_cast<StreamingHashJoin>(join)),
    };
}

QueryPipelineBuilderPtr StreamingJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamingJoinStep expect two input steps");

    return QueryPipelineBuilder::joinPipelinesStreaming(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        max_block_size,
        join_max_wait_ms,
        join_max_wait_rows,
        join_max_cached_bytes,
        &processors);
}

void StreamingJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}
}
