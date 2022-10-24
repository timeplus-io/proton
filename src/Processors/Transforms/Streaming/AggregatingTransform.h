#pragma once

#include <Interpreters/Streaming/Aggregator.h>
/// #include <Processors/IAccumulatingTransform.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeFactory.h>
#include <Core/Streaming/WatermarkInfo.h>

namespace DB
{
namespace Streaming
{
using AggregatorList = std::list<Aggregator>;
using AggregatorListPtr = std::shared_ptr<AggregatorList>;

struct AggregatingTransformParams
{
    Aggregator::Params params;

    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.
    AggregatorListPtr aggregator_list_ptr;
    Aggregator & aggregator;
    bool final;
    bool only_merge = false;
    bool emit_version = false;
    DataTypePtr version_type;

    AggregatingTransformParams(const Aggregator::Params & params_, bool final_, bool emit_version_)
        : params(params_)
        , aggregator_list_ptr(std::make_shared<AggregatorList>())
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), params))
        , final(final_)
        , emit_version(emit_version_)
    {
        if (emit_version)
            version_type = DataTypeFactory::instance().get("int64");
    }

    Block getHeader() const { return aggregator.getHeader(final, false, emit_version); }
};

struct ManyAggregatedData
{
    ManyAggregatedDataVariants variants;

    /// Watermarks for all variants
    std::vector<WatermarkBound> watermarks;
    std::atomic<UInt32> num_finished = 0;
    std::atomic<UInt32> finalizations = 0;
    std::atomic<Int64> version = 0;

    std::condition_variable finalized;
    std::mutex finalizing_mutex;
    WatermarkBound arena_watermark;

    explicit ManyAggregatedData(size_t num_threads = 0) : variants(num_threads), watermarks(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();
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
    AggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const String & log_name);

    /// For Parallel aggregating.
    AggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads,
        const String & log_name);

    ~AggregatingTransform() override;

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    virtual void consume(Chunk chunk);

    virtual void finalize(ChunkInfoPtr) { }

    inline IProcessor::Status preparePushToOutput();
    void initGenerate();

protected:
    void emitVersion(Block & block);
    void setCurrentChunk(Chunk chunk, ChunkInfoPtr & chunk_info);

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

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;
    WatermarkBound & watermark_bound;

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

    /// Aggregated result which is pushed to downstream output
    Chunk current_chunk_aggregated;
    UInt64 rows_since_last_finalization = 0;
    bool has_input = false;
};
}
}
