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

    String getName() const override { return "UserDefinedEmitStrategyAggregatingTransformWithSubstream"; }

private:
    void finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;
};
}
}
