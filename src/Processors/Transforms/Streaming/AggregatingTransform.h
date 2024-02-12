#pragma once

#include <Interpreters/Streaming/Aggregator.h>
#include <Core/Streaming/SubstreamID.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>
#include <Common/serde.h>

#include <any>

namespace DB
{
namespace Streaming
{
struct AggregatingTransformParams
{
    Aggregator aggregator;
    Aggregator::Params & params;
    bool final;
    bool only_merge = false;
    bool emit_version;
    bool emit_changelog;
    DataTypePtr version_type;
    Streaming::EmitMode emit_mode;

    AggregatingTransformParams(const Aggregator::Params & params_, bool final_, bool emit_version_, bool emit_changelog_, Streaming::EmitMode watermark_emit_mode_)
        : aggregator(params_)
        , params(aggregator.getParams())
        , final(final_)
        , emit_version(emit_version_)
        , emit_changelog(emit_changelog_)
        , emit_mode(watermark_emit_mode_)
    {
        if (emit_version)
            version_type = DataTypeFactory::instance().get("int64");
    }

    static Block getHeader(const Aggregator::Params & params, bool final, bool emit_version, bool emit_changelog)
    {
        auto res = params.getHeader(final);
        if (final)
        {
            if (emit_version)
                res.insert({DataTypeFactory::instance().get("int64"), ProtonConsts::RESERVED_EMIT_VERSION});

            if (emit_changelog)
                res.insert({DataTypeFactory::instance().get("int8"), ProtonConsts::RESERVED_DELTA_FLAG});
        }

        return res;
    }

    Block getHeader() const { return getHeader(params, final, emit_version, emit_changelog); }
};

class AggregatingTransform;

SERDE struct ManyAggregatedData
{
    /// Reference to all transforms
    std::vector<AggregatingTransform *> aggregating_transforms;

    std::vector<std::unique_ptr<std::timed_mutex>> variants_mutexes;
    SERDE ManyAggregatedDataVariants variants;

    /// Watermarks for all variants
    /// Acquire lock when update current watemark and find min watermark from all transform
    std::mutex watermarks_mutex;
    SERDE std::vector<Int64> watermarks TSA_GUARDED_BY(watermarks_mutex);

    std::mutex finalizing_mutex;

    /// `finalized_watermark` is capturing the max watermark we have progressed
    SERDE std::atomic<Int64> finalized_watermark = INVALID_WATERMARK;
    SERDE std::atomic<Int64> finalized_window_end = INVALID_WATERMARK;

    SERDE std::atomic<Int64> emited_version = 0;

    SERDE std::vector<std::unique_ptr<std::atomic<UInt64>>> rows_since_last_finalizations;

    std::atomic<UInt32> ckpt_requested = 0;
    std::atomic<AggregatingTransform *> last_checkpointing_transform = nullptr;

    std::atomic<Int64> last_log_ts = MonotonicMilliseconds::now();

    /// Stuff additional data context to it if needed
    SERDE struct AnyField
    {
        SERDE std::any field;
        std::function<void(const std::any &, WriteBuffer &, VersionType)> serializer;
        std::function<void(std::any &, ReadBuffer &, VersionType)> deserializer;
    } any_field;

    explicit ManyAggregatedData(size_t num_threads) : variants(num_threads), watermarks(num_threads, INVALID_WATERMARK)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();

        for (size_t i = 0; i < num_threads; ++i)
        {
            rows_since_last_finalizations.emplace_back(std::make_unique<std::atomic<UInt64>>(0));
            variants_mutexes.emplace_back(std::make_unique<std::timed_mutex>());
        }

        aggregating_transforms.resize(variants.size());
    }

    bool hasField() const { return any_field.field.has_value(); }

    void setField(AnyField && field_) { any_field = std::move(field_); }

    template<typename T>
    T & getField() { return std::any_cast<T &>(any_field.field); }

    template<typename T>
    const T & getField() const { return std::any_cast<const T &>(any_field.field); }

    bool hasNewData() const
    {
        return std::any_of(
            rows_since_last_finalizations.begin(), rows_since_last_finalizations.end(), [](const auto & rows) { return *rows > 0; });
    }

    void resetRowCounts()
    {
        for (auto & rows : rows_since_last_finalizations)
            *rows = 0;
    }

    void addRowCount(size_t rows, size_t current_variant)
    {
        *rows_since_last_finalizations[current_variant] += rows;
    }
};

using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class AggregatingTransform : public IProcessor
{
public:
    AggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_);

    /// For Parallel aggregating.
    AggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads,
        const String & log_name,
        ProcessorID pid_);

    ~AggregatingTransform() override;

    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    friend struct ManyAggregatedData;

private:
    virtual void consume(Chunk chunk);

    virtual void finalize(const ChunkContextPtr &) { }

    inline IProcessor::Status preparePushToOutput();

    void checkpointAlignment(const CheckpointContextPtr &);

    void finalizeAlignment(const ChunkContextPtr &);

    /// returns @p min_watermark
    Int64 updateAndAlignWatermark(Int64 new_watermark);

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    bool propagateWatermarkAndClearExpiredStates();

    /// Try propagate checkpoint to downstream
    bool propagateCheckpointAndReset();

    /// Try propagate an empty rows chunk to downstream, act as a heart beat
    bool propagateHeartbeatChunk();

    /// Try log aggregating metrics
    void logAggregatingMetrics();

    void logAggregatingMetricsWithoutLock(Int64 start_ts = MonotonicMilliseconds::now());

protected:
    void emitVersion(Chunk & chunk);
    /// return {should_abort, need_finalization} pair
    virtual std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, size_t num_rows);
    void setCurrentChunk(Chunk chunk, Chunk retracted_chunk = {});

    /// Quickly check if need finalization
    virtual bool needFinalization(Int64 /*min_watermark*/) const { return true; }

    /// Prepare and check whether can finalization many_data (called after acquired finalizing lock)
    virtual bool prepareFinalization(Int64 /*min_watermark*/) { return true; }

    virtual void clearExpiredState(Int64 /*finalized_watermark*/) { }

    [[nodiscard]] std::vector<std::unique_lock<std::timed_mutex>> lockAllDataVariants();

protected:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    Poco::Logger * log;

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    SERDE ManyAggregatedDataPtr many_data;
    std::timed_mutex & variants_mutex;
    AggregatedDataVariants & variants;
    SERDE Int64 & watermark;

    /// It is used to save the AggregatingTransform has been propagated watermark and garbage collect time bucketed memory for itself:
    /// time buckets which are below this watermark can be safely GCed.
    SERDE Int64 propagated_watermark = INVALID_WATERMARK;

    /// Hold local checkpoint request until all checkpoint request completed, then propagate it to downstream.
    CheckpointContextPtr ckpt_request;

    size_t current_variant;

    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_consume_finished = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    /// Aggregated result which is pushed to downstream output
    Chunk current_chunk_retracted;
    Chunk current_chunk_aggregated;
    bool has_input = false;

    static constexpr auto finalizing_check_interval_ms = std::chrono::milliseconds(10);

    /// If the current thread fails to acquire the finalizing lock, then we keep the watermark and
    /// continue to try in the next processing (it's efficient, avoiding lock waiting)
    std::optional<Int64> try_finalizing_watermark;

    static constexpr Int64 log_metrics_interval_ms = 60'000;
};
}
}
