#pragma once

#include "AggregatingTransform.h"

#include <Core/Streaming/WatermarkInfo.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>

#include <boost/dynamic_bitset.hpp>

namespace DB
{
namespace Streaming
{
const WatermarkBound MAX_WATERMARK
    = WatermarkBound{INVALID_SUBSTREAM_ID, std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::max()};
const WatermarkBound MIN_WATERMARK = WatermarkBound{};

struct SubstreamContext
{
    ManyAggregatedDataVariants many_variants;
    std::shared_mutex variants_mutex;

    boost::dynamic_bitset<> finalized;
    Int64 version = 0;
    std::mutex finalizing_mutex;

    /// Only for tumble/hop
    WatermarkBound min_watermark{MAX_WATERMARK};
    WatermarkBound max_watermark{MIN_WATERMARK};
    WatermarkBound arena_watermark{};

    explicit SubstreamContext(size_t num) : many_variants(num), finalized(num)
    {
        for (auto & elem : many_variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }
};
using SubstreamContextPtr = std::shared_ptr<SubstreamContext>;

struct SubstreamManyAggregatedData : ManyAggregatedData
{
    SubstreamHashMap<SubstreamContextPtr> substream_contexts;
    std::mutex ctx_mutex;

    /// Only for session
    /// FIXME: global sessions maps
    std::vector<std::shared_ptr<SubstreamHashMap<SessionInfoPtr>>> sessions_maps;

    explicit SubstreamManyAggregatedData(size_t num_threads = 0) : ManyAggregatedData(num_threads), sessions_maps(num_threads) { }
};
using SubstraemManyAggregatedDataPtr = std::shared_ptr<SubstreamManyAggregatedData>;

/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class AggregatingTransformWithSubstream : public AggregatingTransform
{
public:
    /// For Parallel aggregating.
    AggregatingTransformWithSubstream(
        Block header,
        AggregatingTransformParamsPtr params_,
        SubstraemManyAggregatedDataPtr substream_many_data,
        size_t current_aggregating_index_,
        size_t max_threads,
        size_t temporary_data_merge_threads,
        const String & log_name);

    ~AggregatingTransformWithSubstream() override = default;

    String getName() const override { return "AggregatingTransformWithSubstream"; }

protected:
    void emitVersion(Block & block, const SubstreamID & id);
    bool executeOrMergeColumns(const Columns & columns, const SubstreamID & id);
    SubstreamContextPtr getSubstreamContext(const SubstreamID & id);
    bool removeSubstreamContext(const SubstreamID & id);


    SubstraemManyAggregatedDataPtr substream_many_data;
    size_t many_aggregating_size;
    size_t current_aggregating_index;
    boost::dynamic_bitset<> all_finalized_mark;
};

}
}
