#include "AggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
AggregatingTransformWithSubstream::AggregatingTransformWithSubstream(
    Block header,
    AggregatingTransformParamsPtr params_,
    SubstreamManyAggregatedDataPtr substream_many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & log_name,
    ProcessorID pid_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        substream_many_data_->many_data,
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        log_name,
        pid_)
    , substream_many_data(std::move(substream_many_data_))
    , many_aggregating_size(substream_many_data->many_data->variants.size())
{
}

void AggregatingTransformWithSubstream::emitVersion(Block & block, const SubstreamID & id)
{
    Int64 version = getOrCreateSubstreamContext(id)->version++;
    block.insert(
        {params->version_type->createColumnConst(block.rows(), version)->convertToFullColumnIfConst(),
         params->version_type,
         ProtonConsts::RESERVED_EMIT_VERSION});
}

bool AggregatingTransformWithSubstream::executeOrMergeColumns(Columns columns, const SubstreamID & id)
{
    /// When the workflow reaches here, the upstream (WatermarkTransformWithSubstream) already splits data
    /// according to partition keys
    const UInt64 num_rows = columns[0]->size();

    /// FIXME: support merge
    assert(!params->only_merge);

    auto substream_ctx = getOrCreateSubstreamContext(id);
    /// Shared variants of current substream for aggregating parallel, which use different variants.
    std::shared_lock lock(substream_ctx->variants_mutex);
    auto & data_variant = substream_ctx->many_variants[current_variant];

    return params->aggregator.executeOnBlock(std::move(columns), num_rows, *data_variant, key_columns, aggregate_columns, no_more_keys);
}

SubstreamContextPtr AggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    /// assert(id != INVALID_SUBSTREAM_ID);

    std::lock_guard lock(substream_many_data->ctx_mutex);

    auto iter = substream_many_data->substream_contexts.find(id);
    if (iter == substream_many_data->substream_contexts.end())
        return substream_many_data->substream_contexts.emplace(id, std::make_shared<SubstreamContext>(many_aggregating_size)).first->second;

    return iter->second;
}

bool AggregatingTransformWithSubstream::removeSubstreamContext(const SubstreamID & id)
{
    std::lock_guard lock(substream_many_data->ctx_mutex);
    return substream_many_data->substream_contexts.erase(id);
}

}
}
