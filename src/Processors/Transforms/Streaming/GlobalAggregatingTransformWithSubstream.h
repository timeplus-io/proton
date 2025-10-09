#pragma once

#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class GlobalAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
{
public:
    GlobalAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id);

    ~GlobalAggregatingTransformWithSubstream() override = default;

    String getName() const override;

protected:
    void initSubstreamContext(const SubstreamAggregatedDataPtr & substream_ctx) override;

private:
    void finalize(const SubstreamAggregatedDataPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;

    /// V1 - Save retract states through additional AggregatedDataVariants (hash table).
    /// V2 - Enable tracking updates with retract, which allows retract states and aggregated states to share the same hash table
    static constexpr VersionType V2 = 14;
    bool retractEnabled(const SubstreamAggregatedDataPtr & substream_ctx) const override;
    void enableRetract(const SubstreamAggregatedDataPtr & substream_ctx) override;
};

}
}
