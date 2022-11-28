#pragma once

#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class TumbleHopAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
{
public:
    TumbleHopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    TumbleHopAggregatingTransformWithSubstream(
        Block header,
        AggregatingTransformParamsPtr params_,
        SubstreamManyAggregatedDataPtr substream_many_data,
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~TumbleHopAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "TumbleHopAggregatingTransformWithSubstream"; }

private:
    void consume(Chunk chunk) override;

    void finalize(ChunkContextPtr chunk_ctx) override;

    inline void doFinalize(const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void convertTwoLevel(ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx);

    inline void removeBuckets(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark);

    std::tuple<WatermarkBound, WatermarkBound, WatermarkBound>
    finalizeAndGetWatermarks(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark_bound);

private:
    WatermarkBound prev_arena_watermark;
};

}
}
