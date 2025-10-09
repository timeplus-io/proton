#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
class UserDefinedEmitStrategyAggregatingTransform final : public AggregatingTransform
{
public:
    UserDefinedEmitStrategyAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id);

    UserDefinedEmitStrategyAggregatingTransform(
        Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data, size_t current_variant_, size_t max_threads);

    ~UserDefinedEmitStrategyAggregatingTransform() override = default;

    String getName() const override;

private:
    void finalize(const ChunkContextPtr & chunk_ctx) override;
};

}
}
