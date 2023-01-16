#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
class TumbleHopAggregatingTransform final : public AggregatingTransform
{
public:
    TumbleHopAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    TumbleHopAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t current_variant_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_);

    ~TumbleHopAggregatingTransform() override = default;

    String getName() const override { return "TumbleHopAggregatingTransform"; }

private:
    void finalize(const ChunkContextPtr & chunk_ctx) override;

    inline void doFinalize(const WatermarkBound & watermark, const ChunkContextPtr & chunk_ctx);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void convertTwoLevel(ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, const ChunkContextPtr & chunk_ctx);

    inline void removeBuckets();
};
}
}
