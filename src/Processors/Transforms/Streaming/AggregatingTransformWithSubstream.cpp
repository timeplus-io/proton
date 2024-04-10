#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Streaming/AggregatedDataMetrics.h>
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

    if (hasAggregatedResult())
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
        Chunk res{getOutputs().front().getHeader().getColumns(), 0};
        setAggregatedResult(res);
        /// Remember to reset `read_current_chunk`
        read_current_chunk = false;
        return;
    }

    Int64 start_ns = MonotonicNanoseconds::now();
    auto chunk_bytes = current_chunk.bytes();
    metrics.processed_bytes += chunk_bytes;

    if (likely(!is_consume_finished))
    {
        SubstreamContextPtr substream_ctx = getOrCreateSubstreamContext(current_chunk.getSubstreamID());
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
    /// Watermark and need_finalization shall not be true at the same time
    /// since when UDA has user defined emit strategy, watermark is disabled
    if (chunk.hasWatermark())
    {
        finalize(substream_ctx, chunk.getChunkContext());
        /// We always propagate the finalized watermark, since the downstream may depend on it.
        /// For example:
        ///     `WITH cte AS (SELECT i, count() FROM test_31_multishards_stream WHERE _tp_time > earliest_ts() PARTITION BY i) SELECT count() FROM cte`
        /// As you can see, the outer global aggregation depends on the periodic watermark of the inner global aggregation
        propagateWatermarkAndClearExpiredStates(substream_ctx);
    }
    else if (need_finalization)
    {
        finalize(substream_ctx, chunk.getChunkContext());
    }
    else if (chunk.requestCheckpoint())
    {
        checkpoint(chunk.getCheckpointContext());
        /// Propagate the checkpoint barrier to all down stream output ports
        Chunk res{getOutputs().front().getHeader().getColumns(), 0, nullptr, chunk.getChunkContext()};
        setAggregatedResult(res);
    }

    if (MonotonicMilliseconds::now() - last_log_ts > log_metrics_interval_ms)
    {
        auto start = MonotonicMilliseconds::now();
        AggregatedDataMetrics aggregated_data_metrics;
        for (const auto & [_, ctx] : substream_contexts)
            params->aggregator.updateMetrics(ctx->variants, aggregated_data_metrics);
        auto end = MonotonicMilliseconds::now();

        LOG_INFO(
            log,
            "Took {} milliseconds to log metrics. Substream metrics: total_substream_count={} hash_buffer_bytes={}; Aggregated data metrics: {}",
            end - start,
            substream_contexts.size(),
            (sizeof(substream_contexts) + substream_contexts.size() * (sizeof(SubstreamContextPtr) + sizeof(SubstreamContext))),
            aggregated_data_metrics.string());

        last_log_ts = end;
    }
}

void AggregatingTransformWithSubstream::propagateWatermarkAndClearExpiredStates(const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);
    if (!hasAggregatedResult())
    {
        auto chunk_ctx = ChunkContext::create();
        chunk_ctx->setSubstreamID(substream_ctx->id);
        chunk_ctx->setWatermark(substream_ctx->finalized_watermark);
        Chunk res{getOutputs().front().getHeader().getColumns(), 0, nullptr, std::move(chunk_ctx)};
        setAggregatedResult(res);
    }
    else
        assert(substream_ctx->finalized_watermark == aggregated_chunks.back().getWatermark());

    clearExpiredState(substream_ctx->finalized_watermark, substream_ctx);
}

void AggregatingTransformWithSubstream::emitVersion(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    size_t rows = chunk.rows();
    if (params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
    {
        /// For UDA with own emit strategy, possibly a block can trigger multiple emits for a substream, each emit cause emitted_version+1
        /// each emit only has one result, therefore we can count emit times by row number
        auto col = params->version_type->createColumn();
        col->reserve(rows);
        for (size_t i = 0; i < rows; i++)
            col->insert(substream_ctx->emitted_version++);
        chunk.addColumn(std::move(col));
    }
    else
    {
        Int64 version = substream_ctx->emitted_version++;
        chunk.addColumn(params->version_type->createColumnConst(rows, version)->convertToFullColumnIfConst());
    }
}

void AggregatingTransformWithSubstream::emitVersion(ChunkList & chunks, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    if (params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
    {
        for (auto & chunk : chunks)
        {
            auto rows = chunk.rows();
            /// For UDA with own emit strategy, possibly a block can trigger multiple emits, each emit cause version+1
            /// each emit only has one result, therefore we can count emit times by row number
            auto col = params->version_type->createColumn();
            col->reserve(rows);
            for (size_t i = 0; i < rows; i++)
                col->insert(substream_ctx->emitted_version++);
            chunk.addColumn(std::move(col));
        }
    }
    else
    {
        Int64 version = substream_ctx->emitted_version++;
        for (auto & chunk : chunks)
            chunk.addColumn(params->version_type->createColumnConst(chunk.rows(), version)->convertToFullColumnIfConst());
    }
}

void AggregatingTransformWithSubstream::setAggregatedResult(Chunk & chunk)
{
    if (hasAggregatedResult())
        throw Exception("Aggregated chunks was already set.", ErrorCodes::LOGICAL_ERROR);

    aggregated_chunks.emplace_back(std::move(chunk));
}

void AggregatingTransformWithSubstream::setAggregatedResult(ChunkList & chunks)
{
    if (hasAggregatedResult())
        throw Exception("Aggregated chunks was already set.", ErrorCodes::LOGICAL_ERROR);

    aggregated_chunks.swap(chunks);
}

std::pair<bool, bool> AggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    assert(substream_ctx);

    /// When the workflow reaches here, the upstream (WatermarkTransformWithSubstream) already splits data
    /// according to partition keys
    auto num_rows = chunk.getNumRows();

    assert(!params->only_merge && !no_more_keys);

    return params->aggregator.executeOnBlock(
        chunk.detachColumns(), 0, num_rows, substream_ctx->variants, key_columns, aggregate_columns);
}

SubstreamContextPtr AggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
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
    output.push(std::move(aggregated_chunks.front()));
    aggregated_chunks.pop_front();
    return Status::PortFull;
}

void AggregatingTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        writeIntBinary(substream_contexts.size(), wb);
        for (const auto & [id, substream_ctx] : substream_contexts)
        {
            assert(id == substream_ctx->id);
            substream_ctx->serialize(wb, getVersion());
        }
    });
}

void AggregatingTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType version_, ReadBuffer & rb) {
        size_t num_substreams;
        readIntBinary(num_substreams, rb);
        substream_contexts.reserve(num_substreams);
        for (size_t i = 0; i < num_substreams; ++i)
        {
            auto substream_ctx = std::make_shared<SubstreamContext>(this);
            substream_ctx->deserialize(rb, version_);

            /// In case when for we had global aggregated some data, but done checkpoint request before finializing
            if (substream_ctx->rows_since_last_finalization > 0) [[unlikely]]
                LOG_WARNING(
                    log,
                    "Last checkpoint state don't be finalized in substream id={}, rows_since_last_finalization={}",
                    substream_ctx->id,
                    substream_ctx->rows_since_last_finalization);

            substream_contexts.emplace(substream_ctx->id, std::move(substream_ctx));
        }
    });
}

void SubstreamContext::serialize(WriteBuffer & wb, VersionType version) const
{
    DB::Streaming::serialize(id, wb);

    variants.serialize(wb, aggregating_transform->params->aggregator);

    DB::writeIntBinary(finalized_watermark, wb);

    DB::writeIntBinary(emitted_version, wb);

    DB::writeIntBinary(rows_since_last_finalization, wb);

    bool has_field = hasField();
    DB::writeBoolText(has_field, wb);
    if (has_field)
        any_field.serializer(any_field.field, wb, version);
}

void SubstreamContext::deserialize(ReadBuffer & rb, VersionType version)
{
    DB::Streaming::deserialize(id, rb);

    variants.deserialize(rb, aggregating_transform->params->aggregator);

    DB::readIntBinary(finalized_watermark, rb);

    DB::readIntBinary(emitted_version, rb);

    DB::readIntBinary(rows_since_last_finalization, rb);

    bool has_field;
    DB::readBoolText(has_field, rb);
    if (has_field)
        any_field.deserializer(any_field.field, rb, version);
}

}
}
