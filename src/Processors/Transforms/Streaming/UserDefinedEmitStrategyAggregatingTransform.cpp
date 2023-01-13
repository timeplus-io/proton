#include "UserDefinedEmitStrategyAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : UserDefinedEmitStrategyAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

UserDefinedEmitStrategyAggregatingTransform::UserDefinedEmitStrategyAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "UserDefinedAggregatingTransform",
        ProcessorID::UserDefinedEmitStrategyAggregatingTransformID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED);
}

void UserDefinedEmitStrategyAggregatingTransform::consume(Chunk chunk)
{
    Block merged_block;
    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        Columns columns = chunk.detachColumns();

        if (!params->aggregator.executeOnEachRow(columns, num_rows, variants, key_columns, aggregate_columns, no_more_keys, merged_block))
            is_consume_finished = true;
    }

    if (merged_block.rows() > 0)
        setCurrentChunk(convertToChunk(merged_block), nullptr);
}
}
}
