#include "AggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
AggregatingTransformWithSubstream::AggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_)
    : IProcessor({std::move(header)}, {params_->getHeader()}, pid_)
    , params(std::move(params_))
    , log(&Poco::Logger::get(log_name))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
{
}

IProcessor::Status AggregatingTransformWithSubstream::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished() || is_consume_finished)
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_input)
        return preparePushToOutput();

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        is_consume_finished = true;
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    current_chunk = input.pull(/*set_not_needed = */ true);
    read_current_chunk = true;

    return Status::Ready;
}

void AggregatingTransformWithSubstream::work()
{
    /// When `chunk` gets here, it generally has 2 cases
    /// 0. Chunk with rows with chunk context (watermark or ckpt request)
    /// 1. Chunk with rows without chunk context (watermark or ckpt request)
    /// 2. Chunk without rows but with chunk context (watermark or ckpt request)
    /// 3. Chunk without watermark and without chunk context (shall be UDA with its own emit strategy only)
    /// For case 3, nobody has interests in dealing with it
    auto num_rows = current_chunk.getNumRows();
    if (num_rows == 0 && !current_chunk.hasChunkContext())
    {
        /// Remember to reset `read_current_chunk`
        read_current_chunk = false;
        return;
    }

    Int64 start_ns = MonotonicNanoseconds::now();
    auto chunk_bytes = current_chunk.bytes();
    metrics.processed_bytes += chunk_bytes;

    if (likely(!is_consume_finished))
    {
        SubstreamContextPtr substream_ctx = nullptr;
        if (const auto & substream_id = current_chunk.getSubstreamID(); substream_id != Streaming::INVALID_SUBSTREAM_ID)
            substream_ctx = getOrCreateSubstreamContext(substream_id);

        if (num_rows > 0)
        {
            substream_ctx->addRowCount(num_rows);
            src_rows += num_rows;
            src_bytes += chunk_bytes;
        }

        consume(std::move(current_chunk), substream_ctx);
    }

    /// Remember to reset `read_current_chunk`
    read_current_chunk = false;

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void AggregatingTransformWithSubstream::consume(Chunk chunk, const SubstreamContextPtr & substream_ctx)
{
    bool done = false, need_finalization = false;

    if (chunk.hasRows())
    {
        assert(substream_ctx);
        if (std::tie(done, need_finalization) = executeOrMergeColumns(chunk, substream_ctx); done)
            is_consume_finished = true;
    }

    /// Since checkpoint barrier is always standalone, it can't coexist with watermark,
    /// we handle watermark and checkpoint barrier separately
    if (chunk.hasWatermark() || need_finalization)
    {
        /// Watermark and need_finalization shall not be true at the same time
        /// since when UDA has user defined emit strategy, watermark is disabled
        assert(!(chunk.hasWatermark() && need_finalization));
        finalize(substream_ctx, chunk.getChunkContext());
    }
    // else if (chunk.requestCheckpoint())
    //     checkpointAlignment(chunk);
}

void AggregatingTransformWithSubstream::emitVersion(Block & block, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    size_t rows = block.rows();
    if (params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
    {
        /// For UDA with own emit strategy, possibly a block can trigger multiple emits for a substream, each emit cause version+1
        /// each emit only has one result, therefore we can count emit times by row number
        auto col = params->version_type->createColumn();
        col->reserve(rows);
        for (size_t i = 0; i < rows; i++)
            col->insert(substream_ctx->version++);
        block.insert({std::move(col), params->version_type, ProtonConsts::RESERVED_EMIT_VERSION});
    }
    else
    {
        Int64 version = substream_ctx->version++;
        block.insert(
            {params->version_type->createColumnConst(rows, version)->convertToFullColumnIfConst(),
             params->version_type,
             ProtonConsts::RESERVED_EMIT_VERSION});
    }
}

void AggregatingTransformWithSubstream::setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx)
{
    if (has_input)
        throw Exception("Current chunk was already set.", ErrorCodes::LOGICAL_ERROR);

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (chunk_ctx)
    {
        /// Aggregation is a stop operator, down stream will and shall establish its own watermark
        /// according to a new timestamp column emitted by the aggregator transform, so we will
        /// need clear watermark here and then propagate the chunk context to down stream.
        /// Then downstream will not be confused with upstream (processed) watermark
        /// FIXME, for global aggregation, we rely on periodic upstream timer to emit watermark,
        /// so we should not clear its watermark (watermark act as a timer). This shall be fixed
        /// by install a timer in down stream of the global aggregation instead of relying on
        /// downstream watermark as timer
        if (params->final && params->params.group_by != Aggregator::Params::GroupBy::OTHER)
            chunk_ctx->clearWatermark();

        current_chunk_aggregated.setChunkContext(std::move(chunk_ctx));
    }
}

std::pair<bool, bool> AggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    /// When the workflow reaches here, the upstream (WatermarkTransformWithSubstream) already splits data
    /// according to partition keys
    auto num_rows = chunk.getNumRows();

    assert(!params->only_merge);

    return params->aggregator.executeOnBlock(
        chunk.detachColumns(), 0, num_rows, substream_ctx->variants, key_columns, aggregate_columns, no_more_keys);
}

SubstreamContextPtr AggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    assert(id != INVALID_SUBSTREAM_ID);

    auto iter = substream_contexts.find(id);
    if (iter == substream_contexts.end())
        return substream_contexts.emplace(id, std::make_shared<SubstreamContext>(id)).first->second;

    return iter->second;
}

bool AggregatingTransformWithSubstream::removeSubstreamContext(const SubstreamID & id)
{
    return substream_contexts.erase(id);
}

IProcessor::Status AggregatingTransformWithSubstream::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}

}
}
