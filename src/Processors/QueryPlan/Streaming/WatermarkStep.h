#pragma once

#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
/// Implement watermark assignment for streaming processing
class WatermarkStep final : public ITransformingStep
{
public:
    WatermarkStep(
        const DataStream & input_stream_,
        ASTPtr query_,
        TreeRewriterResultPtr syntax_analyzer_result_,
        StreamingFunctionDescriptionPtr desc_,
        Poco::Logger * log);

    ~WatermarkStep() override = default;

    String getName() const override { return "WatermarkStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    ASTPtr query;
    TreeRewriterResultPtr syntax_analyzer_result;
    StreamingFunctionDescriptionPtr desc;
    String partition_key;
    Poco::Logger * log;
};
}
