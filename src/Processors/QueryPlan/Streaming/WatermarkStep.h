#pragma once

#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
/// Implement watermark assignment for streaming processing
class WatermarkStep final : public ITransformingStep
{
public:
    WatermarkStep(
        const DataStream & input_stream_,
        Block output_header,
        ASTPtr query_,
        TreeRewriterResultPtr syntax_analyzer_result_,
        FunctionDescriptionPtr desc_,
        bool proc_time,
        Poco::Logger * log);

    ~WatermarkStep() override = default;

    String getName() const override { return "WatermarkStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    ASTPtr query;
    TreeRewriterResultPtr syntax_analyzer_result;
    FunctionDescriptionPtr desc;
    bool proc_time;
    String partition_key;
    Poco::Logger * log;
};
}
}
