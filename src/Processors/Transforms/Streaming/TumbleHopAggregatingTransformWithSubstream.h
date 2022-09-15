#pragma once

#include "AggregatingTransform.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>
#include <Core/Streaming/WatermarkInfo.h>

#include <boost/dynamic_bitset.hpp>

namespace DB
{
namespace Streaming
{
const WatermarkBound MAX_WATERMARK
    = WatermarkBound{INVALID_SUBSTREAM_ID, std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::max()};
const WatermarkBound MIN_WATERMARK = WatermarkBound{};

struct SubstreamContext
{
    ManyAggregatedDataVariants many_variants;
    std::shared_mutex variants_mutex;

    boost::dynamic_bitset<> finalized;
    WatermarkBound min_watermark{MAX_WATERMARK};
    WatermarkBound max_watermark{MIN_WATERMARK};
    WatermarkBound arena_watermark{};
    Int64 version = 0;
    std::mutex finalizing_mutex;

    explicit SubstreamContext(size_t num) : many_variants(num), finalized(num)
    {
        for (auto & elem : many_variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }
};
using SubstreamContextPtr = std::shared_ptr<SubstreamContext>;

struct SubstreamManyAggregatedData : ManyAggregatedData
{
    SubstreamHashMap<SubstreamContextPtr> substream_contexts;
    std::mutex ctx_mutex;

    explicit SubstreamManyAggregatedData(size_t num_threads = 0) : ManyAggregatedData(num_threads) { }
};
using SubstraemManyAggregatedDataPtr = std::shared_ptr<SubstreamManyAggregatedData>;

/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class TumbleHopAggregatingTransformWithSubstream : public AggregatingTransform
{
public:
    TumbleHopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    TumbleHopAggregatingTransformWithSubstream(
        Block header,
        AggregatingTransformParamsPtr params_,
        SubstraemManyAggregatedDataPtr substream_many_data,
        size_t current_aggregating_index_,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~TumbleHopAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "TumbleHopAggregatingTransformWithSubstream"; }

protected:
    void emitVersion(Block & block, const SubstreamID & id);

private:
    void consume(Chunk chunk) override;

    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void removeBuckets(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark);

    SubstreamContextPtr getSubstreamContext(const SubstreamID & id);

    std::tuple<WatermarkBound, WatermarkBound, WatermarkBound>
    finalizeAndGetWatermarks(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark_bound);

private:
    SubstraemManyAggregatedDataPtr substream_many_data;
    size_t many_aggregating_size;
    size_t current_aggregating_index;
    boost::dynamic_bitset<> all_finalized_mark;
    WatermarkBound prev_arena_watermark;
};

}
}
