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
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~TumbleHopAggregatingTransform() override = default;

    String getName() const override { return "TumbleHopAggregatingTransform"; }

private:
    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void removeBuckets();
};
}
}
