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

    ~TumbleHopAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "TumbleHopAggregatingTransformWithSubstream"; }

private:
    void finalize(SubstreamContext & ctx, ChunkContextPtr chunk_ctx) override;

    inline void doFinalize(SubstreamContext & ctx, const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx);

private:
    WatermarkBound prev_arena_watermark;
};

}
}
