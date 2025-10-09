#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>

#include <Interpreters/Streaming/Aggregator/AggregatedDataMetrics.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatorParams.h>
#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>
#include <Interpreters/Streaming/Substream/MemorySubstreamHashMap.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB::Streaming
{
/// The life cycles of the following objects need to be maintained carefully for correct memory recycling during dtor.
/// 1. AggregatingTransformWithSubstream
/// 2. AggregatingTransformParams
/// 3. IAggregator
/// 4. SubstreamHashMap<SubstreamAggregatedDataPtr>
/// 5. IAggregatorDataVariants
///
/// The current references directions are
///  AggregatingTransformWithSubstream -> AggregatingTransformParams -> IAggregator
///                                    -> SubstreamHashMap<SubstreamAggregatedDataPtr> -> IAggregatedDataVariants
/// So the current dtor sequence is the reverse order of the above sequence which are good.
/// Please note, during the dtor of IAggregatorDataVariants, it may reference IAggregator
AggregatingTransformWithSubstream::AggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, size_t id, const String & log_name, ProcessorID pid_)
    : IProcessor({std::move(header)}, {params_->getHeader()}, pid_)
    , params(std::move(params_))
    , transform_id(id)
    , key_columns(params->params->keys_size)
    , aggregate_columns(params->params->aggregates_size)
    , last_log_ts(MonotonicMilliseconds::now())
    , logger(getLogger(log_name))
{
    initSubstreamHashMap();
}

void AggregatingTransformWithSubstream::initSubstreamHashMap()
{
    auto value_serializer = [](const void * value, WriteBuffer & wb) {
        const auto & substream_ctx = *reinterpret_cast<const ValueType *>(value);
        substream_ctx->serialize(wb);
        return ErrorCodes::OK;
    };

    auto value_deserializer = [this](void * value, ReadBuffer & rb) {
        auto & substream_ctx = *reinterpret_cast<ValueType *>(value);
        substream_ctx = std::make_shared<SubstreamAggregatedData>(params->aggregatorType(), this);
        initSubstreamContext(substream_ctx);

        substream_ctx->deserialize(rb);
        /// In case when for we had global aggregated some data, but done checkpoint request before finalizing
        if (substream_ctx->rows_since_last_finalization > 0) [[unlikely]]
            LOG_WARNING(
                logger,
                "Last checkpoint state don't be finalized in substream_id={} transform_id={}, rows_since_last_finalization={}",
                substream_ctx->id,
                transform_id,
                substream_ctx->rows_since_last_finalization);

        return ErrorCodes::OK;
    };

    switch (params->params->aggregatorType())
    {
        case AggregatorType::Memory:
        {
            substream_aggregates_data = std::make_unique<Streaming::MemorySubstreamHashMap<ValueType>>(
                std::move(value_serializer), std::move(value_deserializer));
            return;
        }
        case AggregatorType::Hybrid:
        {
            const auto & hybrid_params = std::static_pointer_cast<HybridAggregatorParams>(params->params);
            substream_aggregates_data = std::make_unique<Streaming::HybridSubstreamHashMap<ValueType>>(
                fmt::format("{}-{}_substream", hybrid_params->spill_dir_path, transform_id),
                hybrid_params->max_hot_key_count,
                std::move(value_serializer),
                std::move(value_deserializer),
                logger);
            return;
        }
    }

    UNREACHABLE();
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
        logAggregatingMetrics();
        return;
    }

    Stopwatch stopwatch;
    auto chunk_bytes = current_chunk.bytes();
    metrics.processed_bytes += chunk_bytes;
    metrics.processed_rows += num_rows;

    if (likely(!is_consume_finished))
    {
        SubstreamAggregatedDataPtr substream_ctx = getOrCreateSubstreamContext(current_chunk.getSubstreamID());
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

    metrics.processed_time_ns += stopwatch.elapsedNanoseconds();

    logAggregatingMetrics();
}

void AggregatingTransformWithSubstream::consume(Chunk chunk, const SubstreamAggregatedDataPtr & substream_ctx)
{
    bool done = false, need_finalization = false;

    if (chunk.hasRows())
    {
        assert(substream_ctx);
        if (std::tie(done, need_finalization) = executeOrMergeColumns(chunk, substream_ctx); done)
            is_consume_finished = true;
    }
    else
    {
        /// MarkSource is always generating the historical data start / end mark in a separate and empty chunk
        if (!backfill_done)
        {
            if (!backfill_started)
                backfill_started |= chunk.isHistoricalDataStart();
            else
                backfill_done |= chunk.isHistoricalDataEnd();
        }

        /// We cannot propagate them historical data start / end mark to downstream since they are not valid anymore after aggregating.
        chunk.clearHistoricalDataStartAndEnd();
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
}

void AggregatingTransformWithSubstream::propagateWatermarkAndClearExpiredStates(const SubstreamAggregatedDataPtr & substream_ctx)
{
    assert(substream_ctx);
    if (!hasAggregatedResult())
    {
        Chunk res{getOutputs().front().getHeader().getColumns(), 0};
        if (substream_ctx->finalized_watermark != INVALID_WATERMARK)
        {
            res.setSubstreamID(substream_ctx->id);
            res.setWatermark(substream_ctx->finalized_watermark);
        }
        setAggregatedResult(res);
    }
    else
        assert(substream_ctx->finalized_watermark == aggregated_chunks.back().getWatermark());

    clearExpiredState(substream_ctx->finalized_watermark, substream_ctx);
}

void AggregatingTransformWithSubstream::logAggregatingMetrics()
{
    if (MonotonicMilliseconds::now() - last_log_ts < log_metrics_interval_ms)
        return;

    auto start = MonotonicMilliseconds::now();
    AggregatedDataMetrics aggregated_data_metrics;
    substream_aggregates_data->forEachKeyValue(
        [&, this](const auto & _, auto value) {
            const auto & ctx = *reinterpret_cast<const SubstreamAggregatedDataPtr *>(value.getMapped());
            ctx->variants->updateMetrics(aggregated_data_metrics, params->aggregator->totalSizeOfAggregatedStates());
        },
        /*inmemory=*/true);

    auto end = MonotonicMilliseconds::now();

    LOG_INFO(
        logger,
        "Took {} milliseconds to log metrics, transform_id={}. Substream metrics: {}; Inmemory aggregated data metrics: {}",
        end - start,
        transform_id,
        substream_aggregates_data->metricsString(),
        aggregated_data_metrics.string());

    last_log_ts = end;
}

void AggregatingTransformWithSubstream::emitVersion(Chunk & chunk, const SubstreamAggregatedDataPtr & substream_ctx)
{
    assert(substream_ctx);

    size_t rows = chunk.rows();
    if (params->params->group_by == IAggregatorParams::GroupBy::UserDefined)
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

void AggregatingTransformWithSubstream::emitVersion(ChunkList & chunks, const SubstreamAggregatedDataPtr & substream_ctx)
{
    assert(substream_ctx);

    if (params->params->group_by == IAggregatorParams::GroupBy::UserDefined)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregated chunks was already set.");

    aggregated_chunks.emplace_back(std::move(chunk));
}

void AggregatingTransformWithSubstream::setAggregatedResult(ChunkList & chunks)
{
    if (hasAggregatedResult())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregated chunks was already set.");

    /// If we have multiple chunks, we need to make sure they have the same substream_id (last chunk must have substream_id)
    if (chunks.size() > 1)
    {
        assert(chunks.back().hasChunkContext());
        const auto & substream_id = chunks.back().getSubstreamID();
        for (auto & chunk : chunks)
            chunk.setSubstreamID(substream_id);
    }
    aggregated_chunks.swap(chunks);
}

std::pair<size_t, size_t> AggregatingTransformWithSubstream::chunksAndRowsOfAggregateResults() const noexcept
{
    size_t rows = 0;
    for (const auto & chunk : aggregated_chunks)
        rows += chunk.rows();

    return {aggregated_chunks.size(), rows};
}

std::pair<bool, bool>
AggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamAggregatedDataPtr & substream_ctx)
{
    chassert(substream_ctx);

    /// When the workflow reaches here, the upstream (WatermarkTransformWithSubstream) already splits data
    /// according to partition keys
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// A optimization for emit only updates/changes, caching processed key columns since last finalization (acquired \variants_mutex)
    if (keysCacheEnabled(substream_ctx))
    {
        auto keys = params->aggregator->materializeKeyColumns(columns, key_columns, substream_ctx->variants->isLowCardinality());
        substream_ctx->processed_keys_columns.emplace_back(std::move(keys));
    }

    if (retractEnabled(substream_ctx))
        return params->aggregator->executeAndRetractOnBlock(
            std::move(columns), 0, num_rows, *substream_ctx->variants, key_columns, aggregate_columns, backfilling());
    else
        return params->aggregator->executeOnBlock(
            std::move(columns), 0, num_rows, *substream_ctx->variants, key_columns, aggregate_columns, backfilling());
}

SubstreamAggregatedDataPtr AggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    auto [value, inserted] = substream_aggregates_data->emplaceKey(id);
    auto & substream_ctx = *reinterpret_cast<ValueType *>(value);
    if (inserted)
    {
        substream_ctx = std::make_shared<SubstreamAggregatedData>(params->aggregatorType(), this, id);
        initSubstreamContext(substream_ctx);
    }

    return substream_ctx;
}

bool AggregatingTransformWithSubstream::removeSubstreamContext(const SubstreamID & id)
{
    return substream_aggregates_data->removeKey(id);
}

IProcessor::Status AggregatingTransformWithSubstream::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(aggregated_chunks.front()));
    aggregated_chunks.pop_front();
    return Status::PortFull;
}

}
