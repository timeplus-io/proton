#include <Processors/QueryPlan/Streaming/DelayStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Streaming
{
static Block checkHeaders(const DataStreams & input_streams)
{
    if (input_streams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot delay an empty set of query plan steps");

    Block res = input_streams.front().header;
    for (const auto & stream : input_streams)
        assertBlocksHaveEqualStructure(stream.header, res, "DelayStep");

    return res;
}

DelayStep::DelayStep(DataStreams input_streams_)
    : header(checkHeaders(input_streams_))
{
    input_streams = std::move(input_streams_);

    if (input_streams.size() == 1)
        output_stream = input_streams.front();
    else
        output_stream = DataStream{.header = header, .is_streaming = std::ranges::any_of(input_streams, [](const auto & stream) { return stream.is_streaming; })};
}

QueryPipelineBuilderPtr DelayStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    auto iter = pipelines.begin();
    auto pipeline = std::move(*(iter++));
    chassert(!pipeline->isStreaming());

    QueryPipelineProcessorsCollector collector(*pipeline, this);
    for (;iter != pipelines.end(); ++iter)
    {
        auto & next_pipeline = *iter;
        chassert(!next_pipeline->isStreaming());
        pipeline->addDelayedPipeline(std::move(*next_pipeline));
    }

    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

    return pipeline;
}

void DelayStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}
}
}
