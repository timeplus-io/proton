#include <Processors/Transforms/Streaming/AggregatingHelper.h>

#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/Streaming/AggregatingTransform.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
namespace
{
Chunk mergeBlocksToChunk(BlocksList && blocks)
{
    Chunk merged_chunk;
    for (auto & block : blocks)
        merged_chunk.append(DB::convertToChunk(std::move(block)));
    return merged_chunk;
}

Chunk convertToChunkImpl(AggregatedDataVariants & data, const AggregatingTransformParams & params, ConvertAction action)
{
    if (data.empty())
        return {};

    auto blocks = params.aggregator.convertToBlocks(data, params.final, action, params.params.max_threads);
    /// FIXME: When global aggr states was converted two level hash table, the merged chunk may be too large
    return mergeBlocksToChunk(std::move(blocks));
}
}

namespace AggregatingHelper
{
Chunk convertToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    return convertToChunkImpl(data, params, ConvertAction::STREAMING_EMIT);
}

Chunk mergeAndConvertToChunk(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    auto blocks = params.aggregator.mergeAndConvertToBlocks(data, params.final, ConvertAction::STREAMING_EMIT, params.params.max_threads);
    /// FIXME: When global aggr states was converted two level hash table, the merged chunk may be too large
    return mergeBlocksToChunk(std::move(blocks));
}

Chunk spliceAndConvertBucketsToChunk(
    AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (buckets.size() == 1)
        return convertToChunk(params.aggregator.convertOneBucketToBlock(data, params.final, ConvertAction::STREAMING_EMIT, buckets[0]));
    else
        return convertToChunk(params.aggregator.spliceAndConvertBucketsToBlock(data, params.final, ConvertAction::INTERNAL_MERGE, buckets));
}

Chunk mergeAndSpliceAndConvertBucketsToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (buckets.size() == 1)
        return convertToChunk(
            params.aggregator.mergeAndConvertOneBucketToBlock(data, params.final, ConvertAction::STREAMING_EMIT, buckets[0]));
    else
        return convertToChunk(
            params.aggregator.mergeAndSpliceAndConvertBucketsToBlock(data, params.final, ConvertAction::INTERNAL_MERGE, buckets));
}

ChunkPair
convertToChangelogChunk(AggregatedDataVariants & data, RetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    if (data.empty())
        return {};

    assert(!retracted_data.empty());

    auto retracted_chunk = convertToChunkImpl(retracted_data, params, ConvertAction::RETRACTED_EMIT);
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.getOrCreateChunkContext()->setRetractedDataFlag();
    }

    auto chunk = convertToChunkImpl(data, params, ConvertAction::STREAMING_EMIT);
    if (chunk)
    {
        auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
        chunk.addColumn(std::move(delta_col));
    }

    return {std::move(retracted_chunk), std::move(chunk)};
}

ChunkPair mergeAndConvertToChangelogChunk(
    ManyAggregatedDataVariants & data, ManyRetractedDataVariants & retracted_data, const AggregatingTransformParams & params)
{
    auto [merged_data, merged_retracted_data] = params.aggregator.mergeRetractedGroups(data, retracted_data);
    if (!merged_data)
        return {};

    assert(merged_retracted_data);
    return convertToChangelogChunk(*merged_data, *merged_retracted_data, params);
}
}
}
}
