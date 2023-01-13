#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class UserDefinedEmitStrategyAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
{
public:
    UserDefinedEmitStrategyAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    ~UserDefinedEmitStrategyAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "UserDefinedAggregatingTransformWithSubstream"; }

private:
    void consume(SubstreamContext & ctx, Chunk chunk) override;
};

}
}
