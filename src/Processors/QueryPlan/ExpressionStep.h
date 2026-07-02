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

/// proton: True iff `actions` keeps every shuffle key intact (same name, identity-derived),
/// so an upstream shuffle_description still describes the output. Shared by Expression/FilterStep.
bool expressionPreservesShuffleKeys(const ActionsDAGPtr & actions, const ShuffleDescription & shuffle_description);

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
        const std::string & kv_options = "");
    /// proton: ends.

    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    void updateOutputStream() override;
    /// proton: starts.
    void preserveShuffleDescriptionIfValid(const DataStream & input_stream);
    /// proton: ends.

    ActionsDAGPtr actions_dag;

    /// proton: starts.
    HashTableType hash_table_type;
    std::string spill_dir;
    std::string kv_options;
    size_t max_hot_keys;
    /// proton: ends.
};

}
