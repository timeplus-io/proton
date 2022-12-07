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
    Int64 start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += current_chunk.bytes();

    assert(current_chunk.hasChunkContext());

    if (likely(!is_consume_finished))
    {
        auto & substream_ctx = *getOrCreateSubstreamContext(current_chunk.getSubstreamID());
        auto num_rows = current_chunk.getNumRows();
        if (num_rows > 0)
        {
            substream_ctx.addRowCount(num_rows);
            src_rows += num_rows;
            src_bytes += current_chunk.bytes();
        }

        consume(substream_ctx, std::move(current_chunk));

        read_current_chunk = false;
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void AggregatingTransformWithSubstream::consume(SubstreamContext & ctx, Chunk chunk)
{
    if (chunk.hasRows())
    {
        if (!executeOrMergeColumns(ctx, chunk.detachColumns()))
            is_consume_finished = true;
    }

    /// Since checkpoint barrier is always standalone, it can't coexist with watermark,
    /// we handle watermark and checkpoint barrier separately
    if (chunk.hasWatermark())
        finalize(ctx, chunk.getChunkContext());
    // else if (chunk.requestCheckpoint())
    //     checkpointAlignment(chunk);
}

void AggregatingTransformWithSubstream::emitVersion(SubstreamContext & ctx, Block & block)
{
    Int64 version = ctx.version++;
    block.insert(
        {params->version_type->createColumnConst(block.rows(), version)->convertToFullColumnIfConst(),
         params->version_type,
         ProtonConsts::RESERVED_EMIT_VERSION});
}

void AggregatingTransformWithSubstream::setCurrentChunk(Chunk chunk, ChunkContextPtr chunk_ctx)
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

bool AggregatingTransformWithSubstream::executeOrMergeColumns(SubstreamContext & substream_ctx, Columns columns)
{
    /// When the workflow reaches here, the upstream (WatermarkTransformWithSubstream) already splits data
    /// according to partition keys
    auto num_rows = columns[0]->size();

    assert(!params->only_merge);

    return params->aggregator.executeOnBlock(
        std::move(columns), 0, num_rows, substream_ctx.variants, key_columns, aggregate_columns, no_more_keys);
}

SubstreamContextPtr AggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    assert(id != INVALID_SUBSTREAM_ID);

    auto iter = substream_contexts.find(id);
    if (iter == substream_contexts.end())
        return substream_contexts.emplace(id, std::make_shared<SubstreamContext>()).first->second;

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
