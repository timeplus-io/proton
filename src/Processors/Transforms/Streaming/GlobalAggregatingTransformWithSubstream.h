#pragma once

#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class GlobalAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
{
public:
    GlobalAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    ~GlobalAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "GlobalAggregatingTransformWithSubstream"; }

private:
    void finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;

    inline void doFinalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx);
};

}
}
