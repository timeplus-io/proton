#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

/// proton: starts.
#include <Core/HashTableType.h>
/// proton: ends.

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:
    /// proton: starts.
    explicit ExpressionStep(
        const DataStream & input_stream_,
        const ActionsDAGPtr & actions_dag_,
        HashTableType hash_table_type_ = HashTableType::Memory,
        const std::string & spill_dir_ = "",
        size_t max_hot_keys_ = 0,
        bool preserves_substream = true);
    /// proton: ends.

    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    void updateOutputStream() override;

    ActionsDAGPtr actions_dag;

    /// proton: starts.
    HashTableType hash_table_type;
    std::string spill_dir;
    size_t max_hot_keys;
    /// proton: ends.
};

}
