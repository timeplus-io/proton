#include "AggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
AggregatingTransformWithSubstream::AggregatingTransformWithSubstream(
    Block header,
    AggregatingTransformParamsPtr params_,
    SubstraemManyAggregatedDataPtr substream_many_data_,
    size_t current_aggregating_index_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & log_name)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        substream_many_data_,
        current_aggregating_index_,
        max_threads_,
        temporary_data_merge_threads_,
        log_name)
    , substream_many_data(std::move(substream_many_data_))
    , many_aggregating_size(substream_many_data->variants.size())
    , current_aggregating_index(current_aggregating_index_)
    , all_finalized_mark(many_aggregating_size, 1)
{
}

void AggregatingTransformWithSubstream::emitVersion(Block & block, const SubstreamID & id)
{
    Int64 version = getSubstreamContext(id)->version++;
    block.insert(
        {params->version_type->createColumnConst(block.rows(), version)->convertToFullColumnIfConst(),
         params->version_type,
         ProtonConsts::RESERVED_EMIT_VERSION});
}

bool AggregatingTransformWithSubstream::executeOrMergeColumns(const Columns & columns, const SubstreamID & id)
{
    const UInt64 num_rows = columns.at(0)->size();

    /// TODO: support merge
    assert(!params->only_merge);

    auto ctx = getSubstreamContext(id);
    /// Shared variants of current substream for aggregating parallel, which use different variants.
    std::shared_lock lock(ctx->variants_mutex);
    if (!params->aggregator.executeOnBlock(
            columns, num_rows, *(ctx->many_variants[current_aggregating_index]), key_columns, aggregate_columns, no_more_keys))
        return false;
    return true;
}

SubstreamContextPtr AggregatingTransformWithSubstream::getSubstreamContext(const SubstreamID & id)
{
    std::lock_guard<std::mutex> lock(substream_many_data->ctx_mutex);
    auto iter = substream_many_data->substream_contexts.find(id);
    if (iter == substream_many_data->substream_contexts.end())
        return substream_many_data->substream_contexts.emplace(id, std::make_shared<SubstreamContext>(many_aggregating_size)).first->second;

    return iter->second;
}

bool AggregatingTransformWithSubstream::removeSubstreamContext(const SubstreamID & id)
{
    std::lock_guard<std::mutex> lock(substream_many_data->ctx_mutex);
    return substream_many_data->substream_contexts.erase(id);
}

}
}
