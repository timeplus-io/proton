#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Processors/Transforms/Streaming/AggregatingTransformParams.h>
#include <Common/ProtonCommon.h>

namespace DB::Streaming
{

namespace
{
void updateOutputHeader(Block & res, bool emit_version, bool emit_changelog)
{
    if (emit_version)
        res.insert({DataTypeFactory::instance().get("int64"), ProtonConsts::RESERVED_EMIT_VERSION});

    if (emit_changelog)
        res.insert({DataTypeFactory::instance().get("int8"), ProtonConsts::RESERVED_DELTA_FLAG});
}

IAggregatorPtr createAggregator(const Block & header, const IAggregatorParamsPtr & params)
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Hybrid:
            return std::make_shared<HybridAggregator>(header, std::static_pointer_cast<HybridAggregatorParams>(params));
        case AggregatorType::Memory:
            return std::make_shared<MemoryAggregator>(header, std::static_pointer_cast<MemoryAggregatorParams>(params));
    }
}

}

AggregatingTransformParams::AggregatingTransformParams(
    const Block & header_,
    const IAggregatorParamsPtr & params_,
    bool emit_version_,
    bool emit_changelog_,
    bool emit_repeat_,
    bool emit_delta_,
    Streaming::EmitMode emit_mode_,
    bool keys_already_sharded_,
    bool aggregation_backfill_key_unique_)
    : aggregator(createAggregator(header_, params_))
    , params(params_)
    , emit_version(emit_version_)
    , emit_changelog(emit_changelog_)
    , emit_repeat(emit_repeat_)
    , emit_delta(emit_delta_)
    , emit_mode(emit_mode_)
    , keys_already_sharded(keys_already_sharded_)
    , aggregation_backfill_key_unique(aggregation_backfill_key_unique_)
{
    if (emit_version)
        version_type = DataTypeFactory::instance().get("int64");
}

Block AggregatingTransformParams::getHeader(const Block & header, const IAggregatorParams & params, bool emit_version, bool emit_changelog)
{
    auto res = params.getHeader(header, /*final_=*/true);
    updateOutputHeader(res, emit_version, emit_changelog);

    return res;
}

Block AggregatingTransformParams::getHeader() const
{
    auto res = aggregator->getHeader(/*final_=*/true);
    updateOutputHeader(res, emit_version, emit_changelog);

    return res;
}

}
