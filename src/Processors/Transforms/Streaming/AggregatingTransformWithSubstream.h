#pragma once

#include "AggregatingTransform.h"

#include <Core/Streaming/WatermarkInfo.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>

#include <any>

namespace DB
{
namespace Streaming
{
struct SubstreamContext
{
    AggregatedDataVariants variants;
    Int64 version = 0;
    UInt64 rows_since_last_finalization = 0;

    std::any field;  /// Stuff additional data context to it if needed

    bool hasField() const { return field.has_value(); }

    template<typename T>
    void setField(T && field_) { field = field_; }

    template<typename T>
    T & getField() { return std::any_cast<T &>(field); }

    template<typename T>
    const T & getField() const { return std::any_cast<const T &>(field); }

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

private:
    virtual void consume(SubstreamContext & ctx, Chunk chunk);

    virtual void finalize(SubstreamContext &, ChunkContextPtr) { }

protected:
    void emitVersion(SubstreamContext & ctx, Block & block);
    bool executeOrMergeColumns(SubstreamContext & ctx, Columns columns);
    void setCurrentChunk(Chunk chunk, ChunkContextPtr chunk_ctx);

    virtual SubstreamContextPtr getOrCreateSubstreamContext(const SubstreamID & id);
    bool removeSubstreamContext(const SubstreamID & id);

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
    Chunk current_chunk_aggregated;
    bool has_input = false;

    SubstreamHashMap<SubstreamContextPtr> substream_contexts;
};

}
}
