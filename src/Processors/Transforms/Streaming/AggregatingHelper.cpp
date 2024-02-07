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
    size_t total_rows = 0;
    for (auto & block : blocks)
        total_rows += block.rows();

    if (total_rows == 0)
        return {};

    auto columns = blocks.front().cloneEmptyColumns();
    for (auto & column : columns)
        column->reserve(total_rows);

    Chunk merged_chunk(std::move(columns), 0);
    for (auto & block : blocks)
        merged_chunk.append(DB::convertToChunk(std::move(block)));
    return merged_chunk;
}

Chunk convertToChunkImpl(AggregatedDataVariants & data, const AggregatingTransformParams & params, ConvertType type)
{
    if (data.empty())
        return {};

    BlocksList blocks;
    switch (type)
    {
        case ConvertType::Updates:
        {
            blocks = params.aggregator.convertUpdatesToBlocks(data);
            break;
        }
        case ConvertType::Retract:
        {
            blocks = params.aggregator.convertRetractToBlocks(data);
            break;
        }
        case ConvertType::Normal:
        {
            blocks = params.aggregator.convertToBlocks(data, params.final, params.params.max_threads);
            break;
        }
    }

    /// FIXME: When global aggr states was converted two level hash table, the merged chunk may be too large
    return mergeBlocksToChunk(std::move(blocks));
}
}

namespace AggregatingHelper
{
Chunk convertToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    return convertToChunkImpl(data, params, ConvertType::Normal);
}

Chunk mergeAndConvertToChunk(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    auto blocks = params.aggregator.mergeAndConvertToBlocks(data, params.final, params.params.max_threads);
    /// FIXME: When global aggr states was converted two level hash table, the merged chunk may be too large
    return mergeBlocksToChunk(std::move(blocks));
}

Chunk spliceAndConvertBucketsToChunk(
    AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return convertToChunk(params.aggregator.spliceAndConvertBucketsToBlock(data, params.final, buckets));
}

Chunk mergeAndSpliceAndConvertBucketsToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return convertToChunk(params.aggregator.mergeAndSpliceAndConvertBucketsToBlock(data, params.final, buckets));
}

ChunkPair convertToChangelogChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.empty())
        return {};

    auto retracted_chunk = convertToChunkImpl(data, params, ConvertType::Retract);
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.setConsecutiveDataFlag();
    }

    auto chunk = convertToChunkImpl(data, params, ConvertType::Updates);
    if (chunk)
    {
        auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
        chunk.addColumn(std::move(delta_col));
    }
    return {std::move(retracted_chunk), std::move(chunk)};
}

ChunkPair mergeAndConvertToChangelogChunk(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.size() == 1)
        return convertToChangelogChunk(*data[0], params);

    ChunkPair results;
    auto & [retracted_chunk, chunk] = results;

    auto merged_retracted_data = params.aggregator.mergeRetractGroups(data);
    if (merged_retracted_data)
    {
        retracted_chunk = convertToChunk(*merged_retracted_data, params);
        if (retracted_chunk)
        {
            auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
            retracted_chunk.addColumn(std::move(retracted_delta_col));
            retracted_chunk.setConsecutiveDataFlag();
        }
    }

    auto merged_updated_data = params.aggregator.mergeUpdateGroups(data);
    if (merged_updated_data)
    {
        chunk = convertToChunk(*merged_updated_data, params);
        if (chunk)
        {
            auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
            chunk.addColumn(std::move(delta_col));
        }
    }
    return results;
}
}
}
}
