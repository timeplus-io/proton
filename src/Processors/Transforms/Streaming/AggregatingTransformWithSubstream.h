#pragma once

#include "AggregatingTransform.h"

#include <Core/Streaming/SubstreamID.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>

#include <any>

namespace DB
{
namespace Streaming
{
class AggregatingTransformWithSubstream;

SERDE struct SubstreamContext
{
    /// Reference to current transform
    AggregatingTransformWithSubstream * aggregating_transform;

    SERDE SubstreamID id;

    SERDE AggregatedDataVariants variants;

    /// `finalized_watermark` is capturing the max watermark we have progressed and 
    /// it is used to garbage collect time bucketed memory : time buckets which 
    /// are below this watermark can be safely GCed.
    SERDE Int64 finalized_watermark = INVALID_WATERMARK;

    SERDE Int64 emitted_version = 0;

    SERDE UInt64 rows_since_last_finalization = 0;

    /// Stuff additional data context to it if needed
    SERDE struct AnyField
    {
        std::any field;
        std::function<void(const std::any &, WriteBuffer &, VersionType)> serializer;
        std::function<void(std::any &, ReadBuffer &, VersionType)> deserializer;
    } any_field;

    explicit SubstreamContext(AggregatingTransformWithSubstream * aggr, const SubstreamID & id_ = {}) : aggregating_transform(aggr), id(id_)
    {
        assert(aggregating_transform);
    }

    void serialize(WriteBuffer & wb, VersionType version) const;

    void deserialize(ReadBuffer & rb, VersionType version);

    bool hasField() const { return any_field.field.has_value(); }

    void setField(AnyField && field_) { any_field = std::move(field_); }

    template<typename T>
    T & getField() { return std::any_cast<T &>(any_field.field); }

    template<typename T>
    const T & getField() const { return std::any_cast<const T &>(any_field.field); }

    bool hasNewData() const { return rows_since_last_finalization > 0; }
    void resetRowCounts() { rows_since_last_finalization = 0; }
    void addRowCount(size_t rows) { rows_since_last_finalization += rows; }
};
using SubstreamContextPtr = std::shared_ptr<SubstreamContext>;

/// For now, substream transforms only process the shuffled data
class AggregatingTransformWithSubstream : public IProcessor
{
public:
    AggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_);

    Status prepare() override;
    void work() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

    friend struct SubstreamContext;

private:
    virtual void consume(Chunk chunk, const SubstreamContextPtr & substream_ctx);

    virtual void finalize(const SubstreamContextPtr &, const ChunkContextPtr &) { }

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    void propagateWatermarkAndClearExpiredStates(const SubstreamContextPtr & substream_ctx);

protected:
    void emitVersion(Chunk & chunk, const SubstreamContextPtr & substream_ctx);
    void emitVersion(ChunkList & chunks, const SubstreamContextPtr & substream_ctx);
    /// return {should_abort, need_finalization} pair
    virtual std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx);
    void setAggregatedResult(Chunk & chunk);
    void setAggregatedResult(ChunkList & chunks);
    bool hasAggregatedResult() const noexcept { return !aggregated_chunks.empty(); }

    virtual SubstreamContextPtr getOrCreateSubstreamContext(const SubstreamID & id);
    bool removeSubstreamContext(const SubstreamID & id);

    virtual void clearExpiredState(Int64 /*finalized_watermark*/, const SubstreamContextPtr &) { }

    inline IProcessor::Status preparePushToOutput();

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

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_consume_finished = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    /// Aggregated result which is pushed to downstream output
    ChunkList aggregated_chunks;

    SubstreamHashMap<SubstreamContextPtr> substream_contexts;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;
};

}
}
