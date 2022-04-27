#include "StreamingAggregatingTransform.h"

namespace DB
{
class TumbleHopAggregatingTransform final : public StreamingAggregatingTransform
{
public:
    TumbleHopAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    TumbleHopAggregatingTransform(
        Block header,
        StreamingAggregatingTransformParamsPtr params_,
        ManyStreamingAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~TumbleHopAggregatingTransform() override = default;

    String getName() const override { return "TumbleHopAggregatingTransform"; }

private:
    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void initialize(ManyStreamingAggregatedDataVariantsPtr & data);

    void mergeTwoLevel(
        ManyStreamingAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info);

    inline void removeBuckets();
};

}
