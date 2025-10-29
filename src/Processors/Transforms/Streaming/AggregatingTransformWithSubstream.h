#pragma once

#include <Processors/Transforms/Streaming/AggregatingTransformParams.h>
#include <Processors/Transforms/Streaming/SubstreamAggregatedData.h>

#include <Core/Streaming/SubstreamID.h>
#include <Interpreters/Streaming/Substream/ISubstreamHashMap.h>
#include <Processors/IProcessor.h>


namespace DB::Streaming
{

/// For now, substream transforms only process the shuffled data
class AggregatingTransformWithSubstream : public IProcessor
{
public:
    AggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id, const String & log_name, ProcessorID pid_);

    Status prepare() override;
    void work() override;

    bool hasState() const override { return true; }
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    friend struct SubstreamAggregatedData;

private:
    virtual void consume(Chunk chunk, const SubstreamAggregatedDataPtr & substream_ctx);

    virtual void finalize(const SubstreamAggregatedDataPtr &, const ChunkContextPtr &) { }

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    void propagateWatermarkAndClearExpiredStates(const SubstreamAggregatedDataPtr & substream_ctx);

    /// Try log aggregating metrics
    void logAggregatingMetrics();

    void initSubstreamHashMap();

protected:
    void emitVersion(Chunk & chunk, const SubstreamAggregatedDataPtr & substream_ctx);
    void emitVersion(ChunkList & chunks, const SubstreamAggregatedDataPtr & substream_ctx);
    /// return {should_abort, need_finalization} pair
    virtual std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, const SubstreamAggregatedDataPtr & substream_ctx);
    void setAggregatedResult(Chunk & chunk);
    void setAggregatedResult(ChunkList & chunks);
    bool hasAggregatedResult() const noexcept { return !aggregated_chunks.empty(); }
    std::pair<size_t, size_t> chunksAndRowsOfAggregateResults() const noexcept;

    SubstreamAggregatedDataPtr getOrCreateSubstreamContext(const SubstreamID & id);
    virtual void initSubstreamContext([[maybe_unused]] const SubstreamAggregatedDataPtr & substream_ctx) { }
    bool removeSubstreamContext(const SubstreamID & id);

    virtual void clearExpiredState(Int64 /*finalized_watermark*/, const SubstreamAggregatedDataPtr &) { }

    inline IProcessor::Status preparePushToOutput();

    virtual bool retractEnabled(const SubstreamAggregatedDataPtr &) const { return false; }
    virtual void enableRetract(const SubstreamAggregatedDataPtr &) { }

    bool keysCacheEnabled(const SubstreamAggregatedDataPtr & substream_ctx) const
    {
        return params->params->enable_keys_cache && substream_ctx->keys_cache_enabled;
    }
    void enableKeysCache(const SubstreamAggregatedDataPtr & substream_ctx) { substream_ctx->keys_cache_enabled = true; }
    /// Only treat backfilling as true when backfill aggregation keys are unique
    bool backfilling() const noexcept { return params->aggregation_backfill_key_unique && backfill_started && !backfill_done; }

    AggregatingTransformParamsPtr params;

    size_t transform_id = 0;

    ColumnRawPtrs key_columns;
    AggregateColumns aggregate_columns;

    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_consume_finished = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    /// Aggregated result which is pushed to downstream output
    ChunkList aggregated_chunks;

    using ValueType = SubstreamAggregatedDataPtr;
    SERDE Streaming::SubstreamHashMapPtr substream_aggregates_data;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;

    bool backfill_started = false;
    bool backfill_done = false;

    LoggerPtr logger;
};

}
