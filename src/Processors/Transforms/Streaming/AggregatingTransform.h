#pragma once

#include <Checkpoint/Checkpoint.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/Streaming/AggregatingTransformParams.h>
#include <Processors/Transforms/Streaming/ManyAggregatedData.h>
#include <Common/Stopwatch.h>
#include <Common/serde.h>

namespace DB::Streaming
{

/// It is for streaming query only. Streaming query never ends.
/// It aggregate streams of blocks in memory and finalize (project) intermediate
/// results periodically or on demand
class AggregatingTransform : public IProcessor
{
public:
    AggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant_,
        size_t max_threads,
        const String & log_name,
        ProcessorID pid_);

    ~AggregatingTransform() override;

    Status prepare() override final;
    void work() override final;

    bool hasState() const override { return true; }
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    friend struct ManyAggregatedData;

private:
    virtual void consume(Chunk chunk);

    virtual void finalize(const ChunkContextPtr &) { }

    inline IProcessor::Status preparePushToOutput();

    void finalizeAlignment(const ChunkContextPtr &);

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    bool propagateWatermarkAndClearExpiredStates();

    void logAggregatingMetricsWithoutLock(Int64 start_ts = MonotonicMilliseconds::now());

    void installRocks();
    RocksPtr getOrCreateRocks();

    CheckpointPtr createFileCheckpoint();
    void recoverFileCheckpoint(CheckpointPtr ckpt);

    CheckpointPtr createRocksCheckpoint();
    void recoverRocksCheckpoint(CheckpointPtr ckpt);

protected:
    void emitVersion(Chunk & chunk);
    void emitVersion(ChunkList & chunks);
    /// return {should_abort, need_finalization} pair
    virtual std::pair<bool, bool> executeColumns(Chunk & chunk, size_t num_rows);
    virtual Chunk executeAndFinalizeColumns(Chunk & chunk, size_t num_rows);
    void setAggregatedResult(Chunk & chunk);
    void setAggregatedResult(ChunkList & chunks);
    bool hasAggregatedResult() const noexcept { return !aggregated_chunks.empty(); }
    std::pair<size_t, size_t> chunksAndRowsOfAggregateResults() const noexcept;

    /// Quickly check if need finalization
    virtual bool needFinalization(Int64 /*min_watermark*/) const { return true; }

    /// Prepare and check whether can finalization many_data (called after acquired finalizing lock)
    /// \return true if finalization is good to proceed, otherwise false
    virtual bool prepareFinalization(Int64 /*min_watermark*/) { return true; }

    virtual void clearExpiredState(Int64 /*finalized_watermark*/) { }

    [[nodiscard]] std::vector<std::unique_lock<std::timed_mutex>> lockAllDataVariants();

    UInt64 nextID() { return ++next_id; }

    virtual bool retractEnabled() const { return false; }
    virtual void enableRetract() { }

    /// Multi-thread unsafe. Call enableKeysCache() only while holding the finalizing lock.
    void enableKeysCache();
    bool keysCacheEnabled() const noexcept { return params->params->enable_keys_cache && keys_cache_enabled; }

    /// Only treat backfilling as true when backfill aggregation keys are unique
    bool backfilling() const noexcept { return params->aggregation_backfill_key_unique && backfill_started && !backfill_done; }

    void checkpointAlignment(const CheckpointContextPtr &);

    /// Try propagate checkpoint to downstream
    bool propagateCheckpointAndReset();

    /// Try propagate an empty rows chunk to downstream, act as a heart beat
    bool propagateHeartbeatChunk();

    /// Try log aggregating metrics
    void logAggregatingMetrics();

protected:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;

    ColumnRawPtrs key_columns;
    AggregateColumns aggregate_columns;

    bool keys_cache_enabled = false;

    SERDE ManyAggregatedDataPtr many_data;
    /// References to current_variants in many_data
    std::timed_mutex & variants_mutex;
    IAggregatedDataVariants & variants;
    SERDE Int64 & watermark;

    /// It is used to save the AggregatingTransform has been propagated watermark and garbage collect time bucketed memory for itself:
    /// time buckets which are below this watermark can be safely GCed.
    SERDE Int64 propagated_watermark = INVALID_WATERMARK;

    /// Hold local checkpoint request until all checkpoint request completed, then propagate it to downstream.
    CheckpointContextPtr ckpt_request;

    size_t current_variant;
    size_t max_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_consume_finished = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    /// Aggregated result which is pushed to downstream output
    ChunkList aggregated_chunks;

    static constexpr auto finalizing_check_interval_ms = std::chrono::milliseconds(10);

    /// If the current thread fails to acquire the finalizing lock, then we keep the watermark and
    /// continue to try in the next processing (it's efficient, avoiding lock waiting)
    std::optional<Int64> try_finalizing_watermark;

    static constexpr Int64 log_metrics_interval_ms = 60'000;

    static std::atomic<UInt64> next_id;
    UInt64 transform_id = nextID();

    bool backfill_started = false;
    bool backfill_done = false;

    LoggerPtr logger;
};

}
