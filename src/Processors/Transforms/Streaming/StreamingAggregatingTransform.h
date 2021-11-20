#pragma once

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/StreamingAggregator.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/Stopwatch.h>

/// proton: starts. Add by proton
namespace DB
{
using StreamingAggregatorList = std::list<StreamingAggregator>;
using StreamingAggregatorListPtr = std::shared_ptr<StreamingAggregatorList>;

struct StreamingAggregatingTransformParams
{
    StreamingAggregator::Params params;

    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.
    StreamingAggregatorListPtr aggregator_list_ptr;
    StreamingAggregator & aggregator;
    bool final;
    bool only_merge = false;

    StreamingAggregatingTransformParams(const StreamingAggregator::Params & params_, bool final_)
        : params(params_)
        , aggregator_list_ptr(std::make_shared<StreamingAggregatorList>())
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), params))
        , final(final_)
    {
    }

    Block getHeader() const { return aggregator.getHeader(final); }

    Block getCustomHeader(bool final_) const { return aggregator.getHeader(final_); }
};

struct ManyStreamingAggregatedData
{
    ManyStreamingAggregatedDataVariants variants;
    std::vector<std::unique_ptr<std::mutex>> mutexes;
    std::atomic<UInt32> num_finished = 0;
    std::atomic<UInt32> finalizations = 0;

    explicit ManyStreamingAggregatedData(size_t num_threads = 0) : variants(num_threads), mutexes(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<StreamingAggregatedDataVariants>();

        for (auto & mut : mutexes)
            mut = std::make_unique<std::mutex>();
    }
};

using ManyStreamingAggregatedDataPtr = std::shared_ptr<ManyStreamingAggregatedData>;
using StreamingAggregatingTransformParamsPtr = std::shared_ptr<StreamingAggregatingTransformParams>;

/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class StreamingAggregatingTransform final : public IProcessor
{
public:
    StreamingAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    StreamingAggregatingTransform(
        Block header,
        StreamingAggregatingTransformParamsPtr params_,
        ManyStreamingAggregatedDataPtr many_data,
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
    void finalize(const ChunkInfo & chunk_info);
    void doFinalize(const ChunkInfo & chunk_info);
    void initialize(ManyStreamingAggregatedDataVariantsPtr & data);
    void mergeSingleLevel(ManyStreamingAggregatedDataVariantsPtr & data);
    void mergeTwoLevel(ManyStreamingAggregatedDataVariantsPtr & data, const ChunkInfo & chunk_info);
    void setCurrentChunk(Chunk chunk);
    IProcessor::Status preparePushToOutput();

private:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    StreamingAggregatingTransformParamsPtr params;
    Poco::Logger * log = &Poco::Logger::get("StreamingAggregatingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    ManyStreamingAggregatedDataPtr many_data;
    StreamingAggregatedDataVariants & variants;
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
