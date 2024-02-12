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
    std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, size_t num_rows) override;
    bool needFinalization(Int64 min_watermark) const override;
    bool prepareFinalization(Int64 min_watermark) override;

    void finalize(const ChunkContextPtr & chunk_ctx) override;

    /// V1 - Save retract states through additional AggregatedDataVariants (hash table).
    /// V2 - Enable tracking updates with retract, which allows retract states and aggregated states to share the same hash table
    static constexpr VersionType V2 = 4;
    bool & retractEnabled() const noexcept;

    bool only_convert_updates = false;
};

}
}
