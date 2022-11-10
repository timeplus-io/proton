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
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~GlobalAggregatingTransform() override = default;

    String getName() const override { return "GlobalAggregatingTransform"; }

private:
    void finalize(ChunkContextPtr chunk_ctx) override;

    inline void doFinalize(ChunkContextPtr & chunk_ctx);

    inline bool initialize(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx);

    void convertSingleLevel(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx);

    void convertTwoLevel(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx);
};

}
}
