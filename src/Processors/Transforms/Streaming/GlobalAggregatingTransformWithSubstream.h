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

protected:
    SubstreamContextPtr getOrCreateSubstreamContext(const SubstreamID & id) override;
    std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx) override;

private:
    void finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;

    static constexpr VersionType V2 = 3;
};

}
}
