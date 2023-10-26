#include <Processors/Transforms/Streaming/AggregatingHelper.h>

#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/Streaming/AggregatingTransform.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
Chunk AggregatingHelper::convertWithoutKey(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    assert(data.type == AggregatedDataVariants::Type::without_key);
    return convertToChunk(params.aggregator.prepareBlockAndFillWithoutKey(data, params.final, false, ConvertAction::STREAMING_EMIT));
}

Chunk AggregatingHelper::mergeAndConvertWithoutKey(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    auto prepared_data = params.aggregator.prepareVariantsToMerge(data);
    if (prepared_data->empty())
        return {};

    assert(prepared_data->at(0)->type == AggregatedDataVariants::Type::without_key);
    params.aggregator.mergeWithoutKeyDataImpl(*prepared_data, ConvertAction::STREAMING_EMIT);
    return convertWithoutKey(*prepared_data->at(0), params);
}

Chunk AggregatingHelper::convertSingleLevel(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    assert(data.type != AggregatedDataVariants::Type::without_key && !data.isTwoLevel());
    return convertToChunk(params.aggregator.prepareBlockAndFillSingleLevel(data, params.final, ConvertAction::STREAMING_EMIT));
}

Chunk AggregatingHelper::mergeAndConvertSingleLevel(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    auto prepared_data = params.aggregator.prepareVariantsToMerge(data);
    if (prepared_data->empty())
        return {};

    AggregatedDataVariantsPtr & first = prepared_data->at(0);
    assert(first->type != AggregatedDataVariants::Type::without_key && !first->isTwoLevel());

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params.aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*prepared_data, ConvertAction::STREAMING_EMIT);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return convertSingleLevel(*first, params);
}

/// Used for emit changelog
ChunkPair AggregatingHelper::convertWithoutKeyToChangelog(
    AggregatedDataVariants & data, RetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    assert(data.type == AggregatedDataVariants::Type::without_key);
    assert(retracted_data.type == AggregatedDataVariants::Type::without_key);

    auto retracted_chunk = convertToChunk(
        params.aggregator.prepareBlockAndFillWithoutKey(retracted_data, params.final, false, ConvertAction::RETRACTED_EMIT));
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.getOrCreateChunkContext()->setRetractedDataFlag();
    }

    auto chunk = convertToChunk(params.aggregator.prepareBlockAndFillWithoutKey(data, params.final, false, ConvertAction::STREAMING_EMIT));
    if (chunk)
    {
        auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
        chunk.addColumn(std::move(delta_col));
    }

    return {std::move(retracted_chunk), std::move(chunk)};
}

ChunkPair AggregatingHelper::mergeAndConvertWithoutKeyToChangelog(
    ManyAggregatedDataVariants & data, ManyRetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    assert(data.at(0)->type == AggregatedDataVariants::Type::without_key);
    assert(retracted_data.at(0)->type == AggregatedDataVariants::Type::without_key);

    auto prepared_data = params.aggregator.prepareVariantsToMerge(data);
    if (prepared_data->empty())
        return {};

    auto prepared_retracted_data = params.aggregator.prepareVariantsToMerge(retracted_data);
    assert(!prepared_retracted_data->empty());

    params.aggregator.mergeWithoutKeyDataImpl(*prepared_retracted_data, ConvertAction::RETRACTED_EMIT);
    params.aggregator.mergeWithoutKeyDataImpl(*prepared_data, ConvertAction::STREAMING_EMIT);
    return convertWithoutKeyToChangelog(*prepared_data->at(0), *prepared_retracted_data->at(0), params);
}

ChunkPair AggregatingHelper::convertSingleLevelToChangelog(
    AggregatedDataVariants & data, RetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    assert(data.type != AggregatedDataVariants::Type::without_key && !data.isTwoLevel());
    assert(retracted_data.type != AggregatedDataVariants::Type::without_key && !retracted_data.isTwoLevel());

    auto retracted_chunk
        = convertToChunk(params.aggregator.prepareBlockAndFillSingleLevel(retracted_data, params.final, ConvertAction::RETRACTED_EMIT));
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.getOrCreateChunkContext()->setRetractedDataFlag();
    }

    auto chunk = convertToChunk(params.aggregator.prepareBlockAndFillSingleLevel(data, params.final, ConvertAction::STREAMING_EMIT));
    if (chunk)
    {
        auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
        chunk.addColumn(std::move(delta_col));
    }

    return {std::move(retracted_chunk), std::move(chunk)};
}

ChunkPair AggregatingHelper::mergeAndConvertSingleLevelToChangelog(
    ManyAggregatedDataVariants & data, ManyRetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    assert(data.at(0)->type != AggregatedDataVariants::Type::without_key && !data.at(0)->isTwoLevel());
    assert(retracted_data.at(0)->type != AggregatedDataVariants::Type::without_key && !retracted_data.at(0)->isTwoLevel());

    auto prepared_data = params.aggregator.prepareVariantsToMerge(data, /*always_merge_into_empty*/ true);
    if (prepared_data->empty())
        return {};

    auto prepared_retracted_data = params.aggregator.prepareVariantsToMerge(retracted_data, /*always_merge_into_empty*/ true);
    assert(!prepared_retracted_data->empty());

    /// To only emit changelog:
    /// 1) Merge retracted groups data into first one
    /// 2) Merge changed groups data into first one (based on retracted groups)
    params.aggregator.mergeRetractedGroups(*prepared_data, *prepared_retracted_data);
    return convertSingleLevelToChangelog(*prepared_data->at(0), *prepared_retracted_data->at(0), params);
}
}
}
