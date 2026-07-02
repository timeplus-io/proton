#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinStep : public ITransformingStep
{
public:
    explicit ArrayJoinStep(const DataStream & input_stream_, ArrayJoinActionPtr array_join_);
    String getName() const override { return "ArrayJoin"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ArrayJoinActionPtr & arrayJoin() const { return array_join; }

private:
    void updateOutputStream() override;
    /// proton: starts. ARRAY JOIN expands rows in place on the same stream and leaves
    /// non-array columns untouched, so the left shuffle survives unless a shuffle key is
    /// itself one of the array-joined columns.
    void preserveShuffleDescriptionIfValid(const DataStream & input_stream);
    /// proton: ends.

    ArrayJoinActionPtr array_join;
};

}
