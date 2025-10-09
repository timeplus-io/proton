#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class UserDefinedEmitStrategyAggregatingTransformWithSubstream final : public AggregatingTransformWithSubstream
{
public:
    UserDefinedEmitStrategyAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id);

    ~UserDefinedEmitStrategyAggregatingTransformWithSubstream() override = default;

    String getName() const override;

private:
    void finalize(const SubstreamAggregatedDataPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;
};
}
}
