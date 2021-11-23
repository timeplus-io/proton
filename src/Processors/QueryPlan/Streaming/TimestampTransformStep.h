#pragma once

#include <Interpreters/StreamingFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
/// Implement watermark assignment for streaming processing
class TimestampTransformStep final : public ITransformingStep
{
public:
    TimestampTransformStep(
        const DataStream & input_stream_,
        Block output_header,
        ExpressionActionsPtr timestamp_expr_,
        const Names & input_columns_);

    ~TimestampTransformStep() override = default;

    String getName() const override { return "TimestampTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    ExpressionActionsPtr timestamp_expr;
    Names input_columns;
};
}
