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
Chunk mergeChunks(ChunkList && chunks)
{
    size_t total_rows = 0;
    for (auto & chunk : chunks)
        total_rows += chunk.rows();

    auto columns = chunks.front().cloneEmptyColumns();
    for (auto & column : columns)
        column->reserve(total_rows);

    Chunk merged_chunk(std::move(columns), 0);
    for (auto & chunk : chunks)
        merged_chunk.append(std::move(chunk));
    return merged_chunk;
}

ChunkList convertBlocksToChunks(BlocksList && blocks)
{
    ChunkList chunks;
    for (auto & block : blocks)
        chunks.push_back(DB::convertToChunk(std::move(block)));
    return chunks;
}

ChunkList convertToChunksImpl(AggregatedDataVariants & data, const AggregatingTransformParams & params, ConvertType type)
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

    return convertBlocksToChunks(std::move(blocks));
}
}

namespace AggregatingHelper
{
ChunkList convertToChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    return convertToChunksImpl(data, params, ConvertType::Normal);
}

ChunkList mergeAndConvertToChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.size() == 1)
        return convertToChunks(*data[0], params);

    BlocksList blocks = params.aggregator.mergeAndConvertToBlocks(data, params.final, params.params.max_threads);

    return convertBlocksToChunks(std::move(blocks));
}

Chunk spliceAndConvertToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return DB::convertToChunk(params.aggregator.spliceAndConvertToBlock(data, params.final, buckets));
}

Chunk mergeAndSpliceAndConvertToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (data.size() == 1)
        return spliceAndConvertToChunk(*data[0], params, buckets);

    return DB::convertToChunk(params.aggregator.mergeAndSpliceAndConvertToBlock(data, params.final, buckets));
}

ChunkList convertUpdatesToChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    return convertToChunksImpl(data, params, ConvertType::Updates);
}

ChunkList mergeAndConvertUpdatesToChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.size() == 1)
        return convertUpdatesToChunks(*data[0], params);

    auto merged_updates_data = params.aggregator.mergeUpdateGroups(data);
    if (merged_updates_data)
        return convertToChunksImpl(*merged_updates_data, params, ConvertType::Normal);

    return {};
}

Chunk spliceAndConvertUpdatesToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return DB::convertToChunk(params.aggregator.spliceAndConvertUpdatesToBlock(data, buckets));
}

Chunk mergeAndSpliceAndConvertUpdatesToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (data.size() == 1)
        return spliceAndConvertUpdatesToChunk(*data[0], params, buckets);

    return DB::convertToChunk(params.aggregator.mergeAndSpliceAndConvertUpdatesToBlock(data, buckets));
}

ChunkList convertToChangelogChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.empty())
        return {};

    ChunkList results;
    auto retracted_chunk = mergeChunks(convertToChunksImpl(data, params, ConvertType::Retract));
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.setConsecutiveDataFlag();
        results.push_back(std::move(retracted_chunk));
    }

    auto chunk = mergeChunks(convertToChunksImpl(data, params, ConvertType::Updates));
    if (chunk)
    {
        auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
        chunk.addColumn(std::move(delta_col));
        results.push_back(std::move(chunk));
    }
    return results;
}

ChunkList mergeAndConvertToChangelogChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.size() == 1)
        return convertToChangelogChunks(*data[0], params);

    ChunkList results;
    auto merged_retracted_data = params.aggregator.mergeRetractGroups(data);
    if (merged_retracted_data)
    {
        auto retracted_chunk = mergeChunks(convertToChunksImpl(*merged_retracted_data, params, ConvertType::Normal));
        if (retracted_chunk)
        {
            auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
            retracted_chunk.addColumn(std::move(retracted_delta_col));
            retracted_chunk.setConsecutiveDataFlag();
            results.push_back(std::move(retracted_chunk));
        }
    }

    auto merged_updated_data = params.aggregator.mergeUpdateGroups(data);
    if (merged_updated_data)
    {
        auto chunk = mergeChunks(convertToChunksImpl(*merged_updated_data, params, ConvertType::Normal));
        if (chunk)
        {
            auto delta_col = ColumnInt8::create(chunk.rows(), Int8(1));
            chunk.addColumn(std::move(delta_col));
            results.push_back(std::move(chunk));
        }
    }
    return results;
}
}
}
}
