#include "StreamingAggregatingTransform.h"

namespace DB
{
class GlobalAggregatingTransform final : public StreamingAggregatingTransform
{
public:
    GlobalAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    GlobalAggregatingTransform(
        Block header,
        StreamingAggregatingTransformParamsPtr params_,
        ManyStreamingAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~GlobalAggregatingTransform() override = default;

    String getName() const override { return "GlobalAggregatingTransform"; }

private:
    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(ChunkInfoPtr & chunk_info);

    inline bool initialize(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);

    void mergeSingleLevel(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);

    void mergeTwoLevel(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);
};

}
