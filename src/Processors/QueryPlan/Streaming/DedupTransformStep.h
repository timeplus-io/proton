#pragma once

#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
/// #include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
class DedupTransformStep final : public ITransformingStep
{
public:
    DedupTransformStep(const DataStream & input_stream_, Block output_header, TableFunctionDescriptionPtr dedup_func_desc_);

    ~DedupTransformStep() override = default;

    String getName() const override { return "DedupTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    TableFunctionDescriptionPtr dedup_func_desc;
};
}
}
