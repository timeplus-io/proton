#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
class GlobalAggregatingTransform final : public AggregatingTransform
{
public:
    GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    GlobalAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~GlobalAggregatingTransform() override = default;

    String getName() const override { return "GlobalAggregatingTransform"; }

private:
    void finalize(ChunkInfoPtr chunk_info) override;

    inline void doFinalize(ChunkInfoPtr & chunk_info);

    inline bool initialize(ManyAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);

    void mergeSingleLevel(ManyAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);

    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info);
};

}
}
