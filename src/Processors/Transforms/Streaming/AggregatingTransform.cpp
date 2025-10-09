#include <Processors/Transforms/Streaming/AggregatingTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Streaming/Aggregator/AggregatedDataMetrics.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{

std::atomic<UInt64> AggregatingTransform::next_id = 0;

/// The life cycles of the following objects need to be maintained carefully for correct memory recycling during dtor.
/// 1. AggregatingTransform
/// 2. AggregatingTransformParams
/// 3. IAggregator
/// 4. ManyAggregateData
/// 5. ManyIAggregatorDataVariants
///
/// The current references directions are
///  AggregatingTransform -> AggregatingTransformParams -> IAggregator
///                       -> ManyAggregatedData -> ManyIAggregatedDataVariants
/// So the current dtor sequence is the reverse order of the above sequence which are good.
/// Please note, during the dtor of ManyIAggregatorDataVariants, it may reference IAggregator
AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    const String & log_name,
    ProcessorID pid_)
    : IProcessor({std::move(header)}, {params_->getHeader()}, pid_)
    , params(std::move(params_))
    , key_columns(params->params->keys_size)
    , aggregate_columns(params->params->aggregates_size)
    , many_data(std::move(many_data_))
    , variants_mutex(*many_data->variants_mutexes[current_variant_])
    , variants(*many_data->variants[current_variant_])
    , watermark(many_data->watermarks[current_variant_])
    , current_variant(current_variant_)
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , logger(getLogger(log_name))
{
    /// Register itself in the many aggregated data
    many_data->aggregating_transforms[current_variant] = this;

    /// Register a rocks instance
    if (params->aggregator->type() == AggregatorType::Hybrid)
        installRocks();
}

AggregatingTransform::~AggregatingTransform() = default;

IProcessor::Status AggregatingTransform::prepare()
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

    /// Only possible after local checkpointed and other AggregatingTransforms don't finish checkpoint yet.
    /// Check it before input, since we can not fetch new data until all checkpoint request completed
    if (ckpt_request)
        return Status::Ready;

    bool need_process = false;
    /// cond-1: Only possible after finalize failed (because other threads were finalizing)
    /// cond-2: Only possible after finalization, need to propagate new watermark
    if (try_finalizing_watermark.has_value() || many_data->finalized_watermark.load(std::memory_order_relaxed) > propagated_watermark)
        need_process = true;

    /// Get chunk from input.
    if (input.isFinished() && !need_process)
    {
        is_consume_finished = true;
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();

        if (need_process)
            return Status::Ready;

        return Status::NeedData;
    }

    current_chunk = input.pull(/*set_not_needed=*/true);
    read_current_chunk = true;

    return Status::Ready;
}

void AggregatingTransform::work()
{
    Int64 start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += current_chunk.bytes();

    if (likely(!is_consume_finished))
    {
        auto num_rows = current_chunk.getNumRows();
        if (num_rows > 0)
        {
            many_data->addRowCount(num_rows, current_variant);
            src_rows += num_rows;
            src_bytes += current_chunk.bytes();
        }

        consume(std::move(current_chunk));

        read_current_chunk = false;
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void AggregatingTransform::consume(Chunk chunk)
{
    bool should_abort = false, need_finalization = false, need_propagate_heartbeat = false;

    auto num_rows = chunk.getNumRows();
    if (num_rows == 0)
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
        /// For example: `global aggregation over tumble window aggregation`
        chunk.clearHistoricalDataStartAndEnd();
        need_propagate_heartbeat = true;
    }

    if (params->emitOnExecute())
    {
        auto finalized_chunk = executeAndFinalizeColumns(chunk, num_rows);
        if (finalized_chunk.hasRows())
            setAggregatedResult(finalized_chunk);
    }
    else if (num_rows > 0)
    {
        /// There indeed has cases where num_rows of a chunk is greater than 0, but
        /// the columns are empty : select count() from stream where a != 0.
        /// So `executeColumns` accepts num_rows as parameter
        std::tie(should_abort, need_finalization) = executeColumns(chunk, num_rows);
        if (should_abort)
            is_consume_finished = true;
    }

    /// Watermark and need_finalization shall not be true at the same time
    /// since when UDA has user defined emit strategy, watermark is disabled
    chassert(!(chunk.hasWatermark() && need_finalization));

    /// Since checkpoint barrier is always standalone, it can't coexist with watermark,
    /// we handle watermark and checkpoint barrier separately
    chassert(!(chunk.hasWatermark() && chunk.requestCheckpoint()));

    if (chunk.hasWatermark())
        finalizeAlignment(chunk.getChunkContext());
    else if (need_finalization)
        finalize(chunk.getChunkContext());
    else if (chunk.requestCheckpoint())
        checkpointAlignment(chunk.getCheckpointContext());

    /// If the last attempt to finalize failed (because other threads were finalizing), then we will continue to try in this processing.
    /// But there is already output, we will try in the next `work()`
    if (try_finalizing_watermark.has_value() && !hasAggregatedResult())
        finalizeAlignment(ChunkContext::create());

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    propagateWatermarkAndClearExpiredStates();

    /// Try propagate checkpoint to downstream
    propagateCheckpointAndReset();

    /// Try propagate an empty rows chunk to downstream
    if (need_propagate_heartbeat)
        propagateHeartbeatChunk();

    logAggregatingMetrics();
}

std::pair<bool, bool> AggregatingTransform::executeColumns(Chunk & chunk, size_t num_rows)
{
    auto columns = chunk.detachColumns();

    /// Blocking finalization during execution on current variant
    std::lock_guard lock{variants_mutex};

    /// A optimization for emit only updates/changes, caching processed key columns since last finalization (acquired \variants_mutex)
    if (keysCacheEnabled())
    {
        auto keys = params->aggregator->materializeKeyColumns(columns, key_columns, variants.isLowCardinality());
        chassert(many_data->processed_keys_columns.size() == many_data->variants.size());
        many_data->processed_keys_columns[current_variant].emplace_back(std::move(keys));
    }

    if (retractEnabled())
        return params->aggregator->executeAndRetractOnBlock(
            std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, backfilling());
    else
        return params->aggregator->executeOnBlock(std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, backfilling());
}

Chunk AggregatingTransform::executeAndFinalizeColumns(Chunk & chunk, size_t num_rows)
{
    chassert(params->emitOnExecute());

    auto columns = chunk.detachColumns();

    Block finalized_block;
    switch (params->emit_mode)
    {
        case EmitMode::AfterKeyExpire:
        {
            finalized_block = params->aggregator->executeAndFinalizeAfterKeyExpire(
                std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, backfilling());
            break;
        }
        case EmitMode::PerEvent:
        {
            finalized_block = params->aggregator->executeAndFinalizePerRow(
                std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, backfilling());
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected emit mode '{}'", params->emit_mode);
        }
    }

    num_rows = finalized_block.rows();
    Chunk finalized_chunk{finalized_block.detachColumns(), num_rows, nullptr, chunk.getChunkContext()};

    if (params->emit_version)
        emitVersion(finalized_chunk);

    return finalized_chunk;
}

void AggregatingTransform::emitVersion(Chunk & chunk)
{
    size_t rows = chunk.rows();
    if (params->params->group_by == IAggregatorParams::GroupBy::UserDefined || params->emit_mode == EmitMode::PerEvent)
    {
        /// 1) For UDA with own emit strategy, possibly a block can trigger multiple emits, each emit cause version+1
        /// each emit only has one result, therefore we can count emit times by row number
        /// 2) For PerEvent emit, each row is a separate result, therefore we can count emit times by row number
        auto col = params->version_type->createColumn();
        col->reserve(rows);
        for (size_t i = 0; i < rows; i++)
            col->insert(many_data->emitted_version++);
        chunk.addColumn(std::move(col));
    }
    else
    {
        Int64 version = many_data->emitted_version++;
        chunk.addColumn(params->version_type->createColumnConst(rows, version)->convertToFullColumnIfConst());
    }
}

void AggregatingTransform::emitVersion(ChunkList & chunks)
{
    if (params->params->group_by == IAggregatorParams::GroupBy::UserDefined || params->emit_mode == EmitMode::PerEvent)
    {
        for (auto & chunk : chunks)
        {
            auto rows = chunk.rows();
            /// 1) For UDA with own emit strategy, possibly a block can trigger multiple emits, each emit cause version+1
            /// each emit only has one result, therefore we can count emit times by row number
            /// 2) For PerEvent emit, each row is a separate result, therefore we can count emit times by row number
            auto col = params->version_type->createColumn();
            col->reserve(rows);
            for (size_t i = 0; i < rows; i++)
                col->insert(many_data->emitted_version++);
            chunk.addColumn(std::move(col));
        }
    }
    else
    {
        Int64 version = many_data->emitted_version++;
        for (auto & chunk : chunks)
            chunk.addColumn(params->version_type->createColumnConst(chunk.rows(), version)->convertToFullColumnIfConst());
    }
}

void AggregatingTransform::setAggregatedResult(Chunk & chunk)
{
    if (hasAggregatedResult())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregated chunks was already set.");

    aggregated_chunks.emplace_back(std::move(chunk));
}

void AggregatingTransform::setAggregatedResult(ChunkList & chunks)
{
    if (hasAggregatedResult())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregated chunks was already set.");

    aggregated_chunks.swap(chunks);
}

std::pair<size_t, size_t> AggregatingTransform::chunksAndRowsOfAggregateResults() const noexcept
{
    size_t rows = 0;
    for (const auto & chunk : aggregated_chunks)
        rows += chunk.rows();

    return {aggregated_chunks.size(), rows};
}

IProcessor::Status AggregatingTransform::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(aggregated_chunks.front()));
    aggregated_chunks.pop_front();
    return Status::PortFull;
}

bool AggregatingTransform::propagateHeartbeatChunk()
{
    if (hasAggregatedResult())
        return false;

    Chunk res{getOutputs().front().getHeader().getColumns(), 0};
    setAggregatedResult(res);
    return true;
}

void AggregatingTransform::finalizeAlignment(const ChunkContextPtr & chunk_ctx)
{
    Stopwatch stopwatch;
    if (chunk_ctx->hasWatermark())
    {
        /// Re-launch current watermark based on the finalized watermark
        if (unlikely(watermark == TIMEOUT_WATERMARK))
        {
            /// Firstly, acquired finalizing lock, blocking update `many_data->finalized_watermark` in other threads
            /// Secondly, acquired watermark lock, blocking watermark alignment in other threads
            /// NOTICE: Keeping the order of locking, since the watermark lock shall be acquired in
            /// `GlobalAggregatingTransform::prepareFinalization()`
            std::scoped_lock lock(many_data->finalizing_mutex, many_data->watermarks_mutex);
            watermark = many_data->finalized_watermark.load(std::memory_order_relaxed);
        }

        auto new_watermark = chunk_ctx->getWatermark();
        if (new_watermark > watermark)
        {
            if (params->params->group_by == IAggregatorParams::GroupBy::Other)
            {
                /// Global aggregation don't need to align watermark
                try_finalizing_watermark = new_watermark;
                std::lock_guard lock(many_data->watermarks_mutex);
                watermark = new_watermark;
            }
            else
            {
                /// Found min watermark to finalize
                std::lock_guard lock(many_data->watermarks_mutex);
                watermark = new_watermark;
                try_finalizing_watermark = std::ranges::min(many_data->watermarks);
            }
        }
        else if (new_watermark < watermark) [[unlikely]]
        {
            LOG_ERROR(logger, "Found outdated watermark. current watermark={}, but got watermark={}", watermark, new_watermark);
        }
        else
        {
            /// When received the same watermark (it may be a periodic watermark), so we still try finalize it
            try_finalizing_watermark = new_watermark;
        }
    }

    if (!try_finalizing_watermark.has_value())
        return;

    /// Already finalized
    if (hasAggregatedResult())
    {
        /// If the last chunk does not have a watermark, we need to set it to the finalized watermark
        if (!aggregated_chunks.back().hasWatermark())
            aggregated_chunks.back().setWatermark(*try_finalizing_watermark);

        try_finalizing_watermark.reset();
        return;
    }

    /// Fastly check can finalize (without lock)
    if (!needFinalization(*try_finalizing_watermark))
    {
        try_finalizing_watermark.reset();
        return;
    }

    std::unique_lock<std::mutex> lock(many_data->finalizing_mutex, std::try_to_lock);
    if (!lock.owns_lock())
        return; /// Anothor thread is finalizing, so we try in next `work()`

    /// After acquired the lock, we need to prepare and check whether `try_finalizing_watermark` has been finalized in by another AggregatingTransform
    if (!prepareFinalization(*try_finalizing_watermark))
    {
        try_finalizing_watermark.reset();
        return;
    }

    /// In case when multiple shard windows aggregate with "emit on update" and only one shard is updated,
    /// `finalize()` cannot proceed due to an invalid watermark.
    /// To address this, the watermark is adjusted to `MIN_WATERMARK = INVALID_WATERMARK + 1`.
    if (*try_finalizing_watermark == INVALID_WATERMARK)
        try_finalizing_watermark = MIN_WATERMARK;

    /// Blocking all variants's processing of AggregatingTransform
    auto lock_holders = lockAllDataVariants();
    if (isCancelled())
        return;

    auto mutate_chunk_ctx = ChunkContext::mutate(chunk_ctx);
    mutate_chunk_ctx->setWatermark(*try_finalizing_watermark);
    finalize(std::move(mutate_chunk_ctx));
    try_finalizing_watermark.reset();

    if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms >= 300)
    {
        auto [aggr_chunks, aggr_rows] = chunksAndRowsOfAggregateResults();

        LOG_INFO(
            logger,
            "Took {} milliseconds to finalize {} shard aggregation, finalized watermark={} aggregated_chunks={} aggregated_rows={}",
            elapsed_ms,
            many_data->variants.size(),
            many_data->finalized_watermark.load(std::memory_order_relaxed),
            aggr_chunks,
            aggr_rows);
    }

    if (MonotonicMilliseconds::now() - many_data->last_log_ts.load(std::memory_order_relaxed) > log_metrics_interval_ms)
        logAggregatingMetricsWithoutLock();
}

bool AggregatingTransform::propagateWatermarkAndClearExpiredStates()
{
    auto finalized_watermark = many_data->finalized_watermark.load(std::memory_order_relaxed);
    if (finalized_watermark > propagated_watermark)
    {
        if (!hasAggregatedResult())
        {
            Chunk res{getOutputs().front().getHeader().getColumns(), 0};
            res.setWatermark(finalized_watermark);
            setAggregatedResult(res);
        }
        else
            assert(finalized_watermark == aggregated_chunks.back().getWatermark());

        clearExpiredState(finalized_watermark);
        propagated_watermark = finalized_watermark;
        return true;
    }
    return false;
}

void AggregatingTransform::checkpointAlignment(const CheckpointContextPtr & ckpt_ctx)
{
    assert(!ckpt_request);
    ckpt_request = std::move(ckpt_ctx);

    bool is_last_checkpoint_transform = false;
    if (many_data->ckpt_requested.fetch_add(1) + 1 == many_data->variants.size())
    {
        is_last_checkpoint_transform = true;
        many_data->last_checkpointing_transform.store(this);
    }

    /// Do checkpoint for itself
    checkpoint(ckpt_request);

    /// Last checkpoint request done, reset the progress of checkpointing
    if (is_last_checkpoint_transform)
    {
        many_data->last_checkpointing_transform.store(nullptr);
        many_data->ckpt_requested.store(0);
    }
}

bool AggregatingTransform::propagateCheckpointAndReset()
{
    /// Since checkpoint barrier is always standalone, it can't coexist with other contexts.
    if (!ckpt_request || hasAggregatedResult())
        return false;

    /// Only all checkpoints request done, then can reset and propagate current ckpt request
    if (many_data->ckpt_requested.load() == 0)
    {
        Chunk res{getOutputs().front().getHeader().getColumns(), 0};
        res.setCheckpointContext(std::move(ckpt_request));
        setAggregatedResult(res);
        assert(!ckpt_request);
        return true;
    }
    return false;
}

void AggregatingTransform::logAggregatingMetrics()
{
    auto start_ts = MonotonicMilliseconds::now();
    if (start_ts - many_data->last_log_ts.load(std::memory_order_relaxed) <= log_metrics_interval_ms)
        return;

    std::unique_lock<std::mutex> lock(many_data->finalizing_mutex, std::try_to_lock);
    if (!lock.owns_lock())
        return; /// Another thread is finalizing, so we try in next `work()`

    /// Check logged by other threads
    if (MonotonicMilliseconds::now() - many_data->last_log_ts.load(std::memory_order_relaxed) <= log_metrics_interval_ms)
        return;

    auto lock_holders = lockAllDataVariants();
    if (isCancelled())
        return;

    logAggregatingMetricsWithoutLock(start_ts);
}

void AggregatingTransform::logAggregatingMetricsWithoutLock(Int64 start_ts)
{
    AggregatedDataMetrics aggregated_data_metrics;
    for (const auto & data_variants : many_data->variants)
        data_variants->updateMetrics(aggregated_data_metrics, params->aggregator->totalSizeOfAggregatedStates());

    auto end_ts = MonotonicMilliseconds::now();
    LOG_INFO(
        logger, "Took {} milliseconds to log metrics. Aggregated data metrics: {}", end_ts - start_ts, aggregated_data_metrics.string());

    many_data->last_log_ts.store(end_ts, std::memory_order_relaxed);
}

std::vector<std::unique_lock<std::timed_mutex>> AggregatingTransform::lockAllDataVariants()
{
    std::vector<std::unique_lock<std::timed_mutex>> lock_holders;
    lock_holders.reserve(many_data->variants_mutexes.size());
    for (auto & mutex : many_data->variants_mutexes)
        lock_holders.emplace_back(*mutex, std::try_to_lock);

    /// Lock for each varitants mutex
    bool all_locks_acquired = true;
    do
    {
        all_locks_acquired = true;
        for (auto & lock_holder : lock_holders)
        {
            if (isCancelled())
                return {};

            if (!lock_holder.owns_lock())
                if (!lock_holder.try_lock_for(finalizing_check_interval_ms))
                    all_locks_acquired = false;
        }
    } while (!all_locks_acquired);

    return lock_holders;
}

void AggregatingTransform::enableKeysCache()
{
    if (params->params->enable_keys_cache && params->params->keys_size != 0 && params->aggregatorType() == AggregatorType::Memory)
    {
        keys_cache_enabled = true;
        if (many_data->processed_keys_columns.empty())
            many_data->processed_keys_columns.resize(many_data->variants.size());
    }
}
}

}
