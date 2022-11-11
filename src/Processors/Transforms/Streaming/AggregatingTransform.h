#pragma once

#include <Interpreters/Streaming/Aggregator.h>
/// #include <Processors/IAccumulatingTransform.h>
#include <Core/Streaming/WatermarkInfo.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

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
    bool emit_version = false;
    DataTypePtr version_type;

    AggregatingTransformParams(const Aggregator::Params & params_, bool final_, bool emit_version_)
        : aggregator(params_), params(aggregator.getParams()), final(final_), emit_version(emit_version_)
    {
        if (emit_version)
            version_type = DataTypeFactory::instance().get("int64");
    }

    Block getHeader() const { return aggregator.getHeader(final, false, emit_version); }
};

class AggregatingTransform;

struct ManyAggregatedData
{
    ManyAggregatedDataVariants variants;

    /// Reference to all transforms
    std::vector<AggregatingTransform *> aggregating_transforms;

    /// Watermarks for all variants
    std::vector<WatermarkBound> watermarks;
    std::atomic<UInt32> num_finished = 0;
    std::atomic<UInt32> finalizations = 0;
    std::atomic<Int64> version = 0;

    std::condition_variable finalized;
    std::mutex finalizing_mutex;
    WatermarkBound arena_watermark;

    std::condition_variable ckpted;
    std::mutex ckpt_mutex;
    std::vector<Int64> ckpt_epochs;
    std::atomic<UInt32> ckpt_requested = 0;

    std::vector<std::unique_ptr<std::atomic<UInt64>>> rows_since_last_finalizations;

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

    explicit ManyAggregatedData(size_t num_threads) : variants(num_threads), watermarks(num_threads), ckpt_epochs(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();

        for (size_t i = 0; i < num_threads; ++i)
            rows_since_last_finalizations.emplace_back(std::make_unique<std::atomic<UInt64>>(0));

        aggregating_transforms.resize(variants.size());
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
    Processors expandPipeline() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    virtual void consume(Chunk chunk);

    virtual void finalize(ChunkContextPtr) { }

    inline IProcessor::Status preparePushToOutput();
    void initGenerate();
    void checkpointAlignment(Chunk & chunk);

protected:
    void emitVersion(Block & block);
    void setCurrentChunk(Chunk chunk, ChunkContextPtr chunk_ctx);

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
    Int64 & ckpt_epoch;
    size_t current_variant;

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
    bool has_input = false;
};
}
}
