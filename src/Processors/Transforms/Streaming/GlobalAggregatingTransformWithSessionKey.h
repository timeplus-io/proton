#include <Processors/Transforms/Streaming/AggregatingTransform.h>

namespace DB::Streaming
{
class GlobalAggregatingTransformWithSessionKey final : public AggregatingTransform
{
public:
    GlobalAggregatingTransformWithSessionKey(Block header, AggregatingTransformParamsPtr params_, const std::string & id);

    ~GlobalAggregatingTransformWithSessionKey() override = default;

    String getName() const override;
};

}
