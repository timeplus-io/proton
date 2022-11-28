#pragma once

#include "AggregatingTransform.h"

#include <Core/Streaming/WatermarkInfo.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace Streaming
{
const WatermarkBound MAX_WATERMARK
    = WatermarkBound{INVALID_SUBSTREAM_ID, std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::max()};
const WatermarkBound MIN_WATERMARK = WatermarkBound{};

struct SubstreamContext
{
    std::shared_mutex variants_mutex;
    ManyAggregatedDataVariants many_variants;

    std::mutex finalizing_mutex;
    std::vector<UInt8> finalized;
    Int64 version = 0;

    /// Only for tumble/hop
    WatermarkBound min_watermark{MAX_WATERMARK};
    WatermarkBound max_watermark{MIN_WATERMARK};
    WatermarkBound arena_watermark{};

    bool allFinalized() const { return std::all_of(finalized.begin(), finalized.end(), [](auto f) { return f; }); }
    void resetFinalized() { std::fill(finalized.begin(), finalized.end(), 0); }

    explicit SubstreamContext(size_t num) : many_variants(num), finalized(num, 0)
    {
        for (auto & elem : many_variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }
};
using SubstreamContextPtr = std::shared_ptr<SubstreamContext>;

struct SubstreamManyAggregatedData
{
    std::mutex ctx_mutex;
    SubstreamHashMap<SubstreamContextPtr> substream_contexts;

    ManyAggregatedDataPtr many_data;

    explicit SubstreamManyAggregatedData(size_t num_threads) : many_data(std::make_shared<ManyAggregatedData>(num_threads)) { }
};
using SubstreamManyAggregatedDataPtr = std::shared_ptr<SubstreamManyAggregatedData>;

/// Multiplex N substreams to max_threads
class AggregatingTransformWithSubstream : public AggregatingTransform
{
public:
    AggregatingTransformWithSubstream(
        Block header,
        AggregatingTransformParamsPtr params_,
        SubstreamManyAggregatedDataPtr substream_many_data,
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads,
        const String & log_name,
        ProcessorID pid_);

protected:
    void emitVersion(Block & block, const SubstreamID & id);
    bool executeOrMergeColumns(Columns columns, const SubstreamID & id);
    SubstreamContextPtr getOrCreateSubstreamContext(const SubstreamID & id);
    bool removeSubstreamContext(const SubstreamID & id);

    SubstreamManyAggregatedDataPtr substream_many_data;
    size_t many_aggregating_size;
};

}
}
