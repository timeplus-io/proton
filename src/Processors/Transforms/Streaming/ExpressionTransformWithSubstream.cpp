#include <Processors/Transforms/Streaming/ExpressionTransformWithSubstream.h>

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>
#include <Interpreters/Streaming/Substream/MemorySubstreamHashMap.h>

#include <ranges>

namespace DB
{
Block ExpressionTransformWithSubstream::transformHeader(Block header, const ActionsDAG & expression)
{
    return expression.updateHeader(std::move(header));
}

ExpressionTransformWithSubstream::ExpressionTransformWithSubstream(
    const Block & header_,
    ExpressionActionsPtr expression_,
    HashTableType hash_table_type,
    const String & spill_dir,
    size_t max_hot_key_count)
    : ISimpleTransform(
          header_, transformHeader(header_, expression_->getActionsDAG()), false, ProcessorID::ExpressionTransformWithSubstreamID)
    , output_chunk_header(outputs.front().getHeader().getColumns(), 0)
    , expression_template(std::move(expression_))
    , stateful_function_builders(expression_template->getStatefulFunctionBuilders())
    , logger(getLogger("ExpressionTransformWithSubstream"))
    , last_log_ts(MonotonicMilliseconds::now())
{
    if (!stateful_function_builders.empty())
    {
        initSubstreamHashMap(hash_table_type, spill_dir, max_hot_key_count);
        setDescription(fmt::format(
            "stateful_funcs={}",
            fmt::join(
                expression_template->getStatefulFunctions() | std::views::transform([](const auto & func) { return func->getName(); }),
                ",")));
    }
}

void ExpressionTransformWithSubstream::initSubstreamHashMap(
    HashTableType hash_table_type, const String & spill_dir, size_t max_hot_key_count)
{
    auto value_serializer = [](const void * value, WriteBuffer & wb) {
        const auto & stateful_functions = *reinterpret_cast<const ValueType *>(value);
        for (auto & func : *stateful_functions)
            func->serialize(wb);
        return ErrorCodes::OK;
    };

    auto value_deserializer = [this](void * value, ReadBuffer & rb) {
        auto & stateful_functions = *reinterpret_cast<ValueType *>(value);
        stateful_functions = std::make_shared<std::vector<ExecutableFunctionPtr>>();
        stateful_functions->reserve(stateful_function_builders.size());
        for (const auto & builder : stateful_function_builders)
        {
            auto func = builder();
            func->deserialize(rb);
            stateful_functions->emplace_back(std::move(func));
        }
        return ErrorCodes::OK;
    };

    switch (hash_table_type)
    {
        case HashTableType::Memory:
        {
            substream_stateful_functions = std::make_unique<Streaming::MemorySubstreamHashMap<ValueType>>(
                std::move(value_serializer), std::move(value_deserializer));
            return;
        }
        case HashTableType::Hybrid:
        {
            substream_stateful_functions = std::make_unique<Streaming::HybridSubstreamHashMap<ValueType>>(
                spill_dir + "_substream", max_hot_key_count, std::move(value_serializer), std::move(value_deserializer), logger);
            return;
        }
    }

    UNREACHABLE();
}

void ExpressionTransformWithSubstream::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
    {
        chunk.setColumns(output_chunk_header.getColumns(), 0);
    }
    else
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        auto expression = getOrCreateSubstreamExpression(chunk.getSubstreamID());
        expression->execute(block, num_rows);

        chunk.setColumns(block.getColumns(), num_rows);
    }

    if (MonotonicMilliseconds::now() - last_log_ts > log_metrics_interval_ms)
    {
        if (substream_stateful_functions)
            LOG_INFO(logger, "Substream metrics: {}", substream_stateful_functions->metricsString());

        last_log_ts = MonotonicMilliseconds::now();
    }
}

ExpressionActionsPtr ExpressionTransformWithSubstream::getOrCreateSubstreamExpression(const Streaming::SubstreamID & substream_id)
{
    /// If there are no stateful functions, we can share the expression template for all substreams.
    if (!substream_stateful_functions)
        return expression_template;

    /// Otherwise, we need to use expression with separate stateful functions for each substream.
    auto [value, inserted] = substream_stateful_functions->emplaceKey(substream_id);
    auto & stateful_functions = *reinterpret_cast<ValueType *>(value);
    if (inserted)
    {
        stateful_functions = std::make_shared<std::vector<ExecutableFunctionPtr>>();
        stateful_functions->reserve(stateful_function_builders.size());
        for (const auto & builder : stateful_function_builders)
            stateful_functions->emplace_back(builder());
    }

    /// Replace stateful functions within the expression template to avoid cloning the entire expression.
    /// This way is safe because the other members in the expression template are stateless.
    expression_template->replaceStatefulFunctions(*stateful_functions);

    return expression_template;
}

}
