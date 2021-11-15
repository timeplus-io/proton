#pragma once

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Stopwatch.h>

/// proton: starts.

namespace DB
{
/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class StreamingAggregatingTransform final : public IProcessor
{
public:
    StreamingAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    StreamingAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~StreamingAggregatingTransform() override;

    String getName() const override { return "StreamingAggregatingTransform"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    void consume(Chunk chunk);
    bool needsFinalization(const Chunk & chunk) const;
    void finalize(Int64 watermark);
    void doFinalize(Int64 watermark);
    void initialize(ManyAggregatedDataVariantsPtr & data);
    void mergeSingleLevel(ManyAggregatedDataVariantsPtr & data);
    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, Int64 watermark);
    void setCurrentChunk(Chunk chunk);
    IProcessor::Status preparePushToOutput();

private:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    Poco::Logger * log = &Poco::Logger::get("StreamingAggregatingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;
    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_generate_initialized = false;
    bool is_consume_finished = false;
    bool is_pipeline_created = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    void initGenerate();

    /// Aggregated result which is pushed to downstream output
    Chunk current_chunk_aggregated;
    UInt64 rows_since_last_finalization = 0;
    bool has_input = false;
    bool finalizing = false;
};
}
