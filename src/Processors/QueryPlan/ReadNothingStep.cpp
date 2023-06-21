#include <Processors/QueryPlan/ReadNothingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

/// proton: starts.
ReadNothingStep::ReadNothingStep(Block output_header, bool is_streaming_)
    : ISourceStep(DataStream{.header = std::move(output_header), .has_single_port = true, .is_streaming = is_streaming_})
/// proton: ends.
{
}

void ReadNothingStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
}

}
