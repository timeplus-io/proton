#include "UserDefinedEmitStrategyAggregatingTransformWithSubstream.h"

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

UserDefinedEmitStrategyAggregatingTransformWithSubstream::UserDefinedEmitStrategyAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "UserDefinedAggregatingTransformWithSubstream",
        ProcessorID::UserDefinedEmitStrategyAggregatingTransformWithSubstreamID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED);
}

void UserDefinedEmitStrategyAggregatingTransformWithSubstream::consume(SubstreamContext & ctx, Chunk chunk)
{
    Block merged_block;
    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        Columns columns = chunk.detachColumns();

        if (!params->aggregator.executeOnEachRow(
                columns, num_rows, ctx.variants, key_columns, aggregate_columns, no_more_keys, merged_block))
            is_consume_finished = true;
    }

    if (merged_block.rows() > 0)
        setCurrentChunk(convertToChunk(merged_block), nullptr);
}
}
}
