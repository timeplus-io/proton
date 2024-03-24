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

ChunkPair convertToChangelogChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params)
{
    if (data.empty())
        return {};

    auto retracted_chunks = convertToChunksImpl(data, params, ConvertType::Retract);
    assert(retracted_chunks.size() == 1);
    auto & retracted_chunk = retracted_chunks.front();
    if (retracted_chunk)
    {
        auto retracted_delta_col = ColumnInt8::create(retracted_chunk.rows(), Int8(-1));
        retracted_chunk.addColumn(std::move(retracted_delta_col));
        retracted_chunk.setConsecutiveDataFlag();
    }

    auto chunks = convertToChunksImpl(data, params, ConvertType::Updates);
    assert(chunks.size() == 1);
    auto & chunk = chunks.front();
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
        auto retracted_chunks = convertToChunksImpl(*merged_retracted_data, params, ConvertType::Normal);
        assert(retracted_chunks.size() == 1);
        retracted_chunk = std::move(retracted_chunks.front());
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
        auto chunks = convertToChunksImpl(*merged_updated_data, params, ConvertType::Normal);
        assert(chunks.size() == 1);
        chunk = std::move(chunks.front());
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
