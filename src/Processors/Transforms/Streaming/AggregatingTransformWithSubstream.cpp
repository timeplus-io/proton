#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>

#include <Checkpoint/CheckpointCoordinator.h>
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
        setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, nullptr);
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
            assert(substream_ctx);
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
    else if (chunk.requestCheckpoint())
    {
        checkpoint(chunk.getCheckpointContext());
        /// Propagate the checkpoint barrier to all down stream output ports
        setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, chunk.getChunkContext());
    }
}

void AggregatingTransformWithSubstream::emitVersion(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    size_t rows = chunk.rows();
    if (params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
    {
        /// For UDA with own emit strategy, possibly a block can trigger multiple emits for a substream, each emit cause version+1
        /// each emit only has one result, therefore we can count emit times by row number
        auto col = params->version_type->createColumn();
        col->reserve(rows);
        for (size_t i = 0; i < rows; i++)
            col->insert(substream_ctx->version++);
        chunk.addColumn(std::move(col));
    }
    else
    {
        Int64 version = substream_ctx->version++;
        chunk.addColumn(params->version_type->createColumnConst(rows, version)->convertToFullColumnIfConst());
    }
}

void AggregatingTransformWithSubstream::setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx, Chunk retracted_chunk)
{
    if (has_input)
        throw Exception("Current chunk was already set.", ErrorCodes::LOGICAL_ERROR);

    if (!chunk)
        return;

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (chunk_ctx)
    {
        /// NOTE: For StremaingShrinkResize of downstream, it's need all inputs propagate watermark then do watermark alignment,
        /// So the watermark cannot be cleared. On the other hand, if the downstream needs to establish its own watermark,
        /// the watermark will be cleared and reassigned in another `WatermarkStamper` of downstream.
        // if (params->final && params->params.group_by != Aggregator::Params::GroupBy::OTHER)
        //     chunk_ctx->clearWatermark();

        current_chunk_aggregated.setChunkContext(std::move(chunk_ctx));
    }

    if (retracted_chunk.rows())
    {
        current_chunk_retracted = std::move(retracted_chunk);
        current_chunk_retracted.getOrCreateChunkContext()->setRetractedDataFlag();
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
        return substream_contexts.emplace(id, std::make_shared<SubstreamContext>(this, id)).first->second;

    return iter->second;
}

bool AggregatingTransformWithSubstream::removeSubstreamContext(const SubstreamID & id)
{
    return substream_contexts.erase(id);
}

IProcessor::Status AggregatingTransformWithSubstream::preparePushToOutput()
{
    auto & output = outputs.front();

    /// At first, push retracted data, then push aggregated data
    if (current_chunk_retracted)
    {
        output.push(std::move(current_chunk_retracted));
        return Status::PortFull;
    }

    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}

void AggregatingTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        writeIntBinary(substream_contexts.size(), wb);
        for (const auto & [id, substream_ctx] : substream_contexts)
        {
            assert(id == substream_ctx->id);
            substream_ctx->serialize(wb);
        }
    });
}

void AggregatingTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
        size_t num_substreams;
        readIntBinary(num_substreams, rb);
        substream_contexts.reserve(num_substreams);
        for (size_t i = 0; i < num_substreams; ++i)
        {
            auto substream_ctx = std::make_shared<SubstreamContext>(this);
            substream_ctx->deserialize(rb);
            substream_contexts.emplace(substream_ctx->id, std::move(substream_ctx));
        }
    });
}

void SubstreamContext::serialize(WriteBuffer & wb) const
{
    DB::Streaming::serialize(id, wb);

    aggregating_transform->params->aggregator.checkpoint(variants, wb);

    DB::writeIntBinary(finalized_watermark, wb);

    DB::writeIntBinary(version, wb);

    DB::writeIntBinary(rows_since_last_finalization, wb);

    bool has_field = hasField();
    DB::writeBoolText(has_field, wb);
    if (has_field)
        any_field.serializer(any_field.field, wb);
}

void SubstreamContext::deserialize(ReadBuffer & rb)
{
    DB::Streaming::deserialize(id, rb);

    aggregating_transform->params->aggregator.recover(variants, rb);

    DB::readIntBinary(finalized_watermark, rb);

    DB::readIntBinary(version, rb);

    DB::readIntBinary(rows_since_last_finalization, rb);

    bool has_field;
    DB::readBoolText(has_field, rb);
    if (has_field)
        any_field.deserializer(any_field.field, rb);
}

}
}
