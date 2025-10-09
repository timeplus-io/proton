#include <Processors/Transforms/Streaming/AggregatingHelper.h>

#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/Streaming/AggregatingTransform.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB::Streaming
{
namespace
{
[[maybe_unused]] Chunk mergeChunks(ChunkList && chunks)
{
    if (chunks.empty())
        return {};

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
    {
        if (block.rows() == 0)
            continue;

        auto is_retract_block = block.isRetract();
        chunks.push_back(DB::convertToChunk(std::move(block)));
        if (is_retract_block)
            chunks.back().setConsecutiveDataFlag();
    }
    return chunks;
}

ChunkList
convertToChunksImpl(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, AggregatingConvertParams & cparams)
{
    if (data_variants.empty())
        return {};

    return convertBlocksToChunks(params.aggregator->convertToBlocks(data_variants, params.params->max_threads, cparams));
}

}

namespace AggregatingHelper
{
ChunkList convertToChunks(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params)
{
    AggregatingConvertParams cparams{AggregatingConvertType::Normal, params.keys_already_sharded, /*clear_state_=*/params.deltaEmit()};
    return convertToChunksImpl(data_variants, params, cparams);
}

ChunkList mergeAndConvertToChunks(ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params)
{
    if (many_data_variants.size() == 1)
        return convertToChunks(*many_data_variants[0], params);

    AggregatingConvertParams cparams{AggregatingConvertType::Normal, params.keys_already_sharded, /*clear_state_=*/params.deltaEmit()};
    return convertBlocksToChunks(params.aggregator->mergeAndConvertToBlocks(many_data_variants, params.params->max_threads, cparams));
}

Chunk spliceAndConvertToChunk(
    IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return convertToChunk(params.aggregator->spliceAndConvertToBlock(data_variants, buckets));
}

Chunk mergeAndSpliceAndConvertToChunk(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (many_data_variants.size() == 1)
        return spliceAndConvertToChunk(*many_data_variants[0], params, buckets);

    return convertToChunk(params.aggregator->mergeAndSpliceAndConvertToBlock(many_data_variants, buckets));
}

ChunkList convertUpdatesToChunks(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, KeyColumns key_columns)
{
    AggregatingConvertParams cparams{AggregatingConvertType::Updates, std::move(key_columns), params.keys_already_sharded};
    return convertToChunksImpl(data_variants, params, cparams);
}

ChunkList mergeAndConvertUpdatesToChunks(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, ManyKeyColumns many_key_columns)
{
    if (many_data_variants.size() == 1)
        return convertUpdatesToChunks(
            *many_data_variants[0], params, many_key_columns.empty() ? KeyColumns{} : std::move(many_key_columns[0]));

    AggregatingConvertParams cparams{AggregatingConvertType::Updates, std::move(many_key_columns), params.keys_already_sharded};
    return convertBlocksToChunks(params.aggregator->mergeAndConvertToBlocks(many_data_variants, params.params->max_threads, cparams));
}

Chunk spliceAndConvertUpdatesToChunk(
    IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    return convertToChunk(params.aggregator->spliceAndConvertUpdatesToBlock(data_variants, buckets));
}

Chunk mergeAndSpliceAndConvertUpdatesToChunk(
    ManyIAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets)
{
    if (data_variants.size() == 1)
        return spliceAndConvertUpdatesToChunk(*data_variants[0], params, buckets);

    return convertToChunk(params.aggregator->mergeAndSpliceAndConvertUpdatesToBlock(data_variants, buckets));
}

ChunkList convertToChangelogChunks(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, KeyColumns keys_hint)
{
    if (data_variants.empty())
        return {};

    AggregatingConvertParams cparams{AggregatingConvertType::Retract, std::move(keys_hint), params.keys_already_sharded};
    return convertBlocksToChunks(params.aggregator->convertToBlocks(data_variants, params.params->max_threads, cparams));
}

ChunkList mergeAndConvertToChangelogChunks(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, ManyKeyColumns many_key_columns)
{
    if (many_data_variants.size() == 1)
        return convertToChangelogChunks(
            *many_data_variants[0], params, many_key_columns.empty() ? KeyColumns{} : std::move(many_key_columns[0]));

    AggregatingConvertParams cparams{AggregatingConvertType::Retract, std::move(many_key_columns), params.keys_already_sharded};
    return convertBlocksToChunks(params.aggregator->mergeAndConvertToBlocks(many_data_variants, params.params->max_threads, cparams));
}

}

}
