#pragma once

#include "AggregatingTransformWithSubstream.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>
#include <Core/Streaming/WatermarkInfo.h>

#include <boost/dynamic_bitset.hpp>

namespace DB
{
namespace Streaming
{
/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class TumbleHopAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
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

private:
    void consume(Chunk chunk) override;

    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void removeBuckets(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark);

    std::tuple<WatermarkBound, WatermarkBound, WatermarkBound>
    finalizeAndGetWatermarks(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark_bound);

private:
    WatermarkBound prev_arena_watermark;
};

}
}
