#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
class UserDefinedEmitStrategyAggregatingTransform final : public AggregatingTransform
{
public:
    UserDefinedEmitStrategyAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    UserDefinedEmitStrategyAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~UserDefinedEmitStrategyAggregatingTransform() override = default;

    String getName() const override { return "UserDefinedEmitStrategyAggregatingTransform"; }

private:
    void finalize(const ChunkContextPtr & chunk_ctx) override;
};

}
}
