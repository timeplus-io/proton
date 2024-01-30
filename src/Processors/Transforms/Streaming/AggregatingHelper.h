#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{
class Block;
class Chunk;

namespace Streaming
{
struct AggregatingTransformParams;
struct AggregatedDataVariants;
using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;

using RetractedDataVariants = AggregatedDataVariants;
using RetractedDataVariantsPtr = std::shared_ptr<RetractedDataVariants>;
using ManyRetractedDataVariants = ManyAggregatedDataVariants;

using ChunkPair = std::pair<Chunk, Chunk>;

namespace AggregatingHelper
{
/// Convert aggregated state to chunk
Chunk convertToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params);
/// Merge many aggregated state and convert them to chunk
Chunk mergeAndConvertToChunk(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

/// Only used for two level
/// splice aggregatd state of multiple buckets and convert them to chunk
Chunk spliceAndConvertBucketsToChunk(
    AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);
/// Merge many aggregated state of multiple threads, then splice aggregatd state of multiple buckets and convert them to chunk
Chunk mergeAndSpliceAndConvertBucketsToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);

/// Only used for emit changelog
/// @brief only convert the state of changed groups (retracted: last state, aggregated: current state)
///  \data: current aggregated state of all groups (contains retracted states and updated states)
/// @returns <retracted_chunk, aggregated_chunk>
/// retracted_chunk: just contains retracted data of changed groups
/// aggregated_chunk: just contains aggregated data of changed groups
ChunkPair convertToChangelogChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params);
ChunkPair mergeAndConvertToChangelogChunk(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);
}

}
}
