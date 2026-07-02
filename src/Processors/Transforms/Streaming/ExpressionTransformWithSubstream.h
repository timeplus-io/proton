#pragma once
#include <Core/HashTableType.h>
#include <Interpreters/Streaming/Substream/ISubstreamHashMap.h>
#include <Processors/ISimpleTransform.h>
#include <Common/serde.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

class ExpressionTransformWithSubstream final : public ISimpleTransform
{
public:
    ExpressionTransformWithSubstream(
        const Block & header_,
        ExpressionActionsPtr expression_,
        HashTableType hash_table_type,
        const String & spill_dir,
        size_t max_hot_key_count,
        const String & kv_options);

    String getName() const override { return "ExpressionTransformWithSubstream"; }

    static Block transformHeader(Block header, const ActionsDAG & expression);

    bool hasState() const override { return static_cast<bool>(substream_stateful_functions); }
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr getOrCreateSubstreamExpression(const Streaming::SubstreamID & substream_id);

    void initSubstreamHashMap(HashTableType hash_table_type, const String & spill_dir, size_t max_hot_key_count, const String & kv_options);

    Chunk output_chunk_header;

    ExpressionActionsPtr expression_template;
    std::vector<std::function<ExecutableFunctionPtr()>> stateful_function_builders;

    using ValueType = std::shared_ptr<std::vector<ExecutableFunctionPtr>>;
    SERDE Streaming::SubstreamHashMapPtr substream_stateful_functions;

    LoggerPtr logger;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;
};

}
