#include <Processors/Transforms/Streaming/AggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class GlobalAggregatingTransform final : public AggregatingTransform
{
public:
    GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id);

    /// For Parallel aggregating.
    GlobalAggregatingTransform(
        Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data, size_t current_variant_, size_t max_threads);

    ~GlobalAggregatingTransform() override = default;

    String getName() const override;

private:
    bool needFinalization(Int64 min_watermark) const override;
    bool prepareFinalization(Int64 min_watermark) override;

    void finalize(const ChunkContextPtr & chunk_ctx) override;

    /// V1 - Save retract states through additional AggregatedDataVariants (hash table).
    /// V2 - Enable tracking updates with retract, which allows retract states and aggregated states to share the same hash table
    static constexpr VersionType V2 = 14;
    bool retractEnabled() const override;
    void enableRetract() override;
};

}
}
