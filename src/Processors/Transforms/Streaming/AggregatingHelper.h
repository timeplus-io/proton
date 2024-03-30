#pragma once

#include <Core/Streaming/Watermark.h>
#include <base/defines.h>
#include <base/types.h>

#include <list>
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
using ChunkList = std::list<Chunk>;

namespace AggregatingHelper
{
/// Convert aggregated state to chunk
ChunkList convertToChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params);
/// Merge many aggregated state and convert them to chunk
ChunkList mergeAndConvertToChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

/// Only used for two level
/// splice aggregatd state of multiple buckets and convert them to chunk
Chunk spliceAndConvertToChunk(AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);
/// Merge many aggregated state of multiple threads, then splice aggregatd state of multiple buckets and convert them to chunk
Chunk mergeAndSpliceAndConvertToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);

/* for EMIT ON UPDATE */
/// Convert aggregated state of update groups tracked to chunk
ChunkList convertUpdatesToChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params);
/// Merge many aggregated state and convert them to chunk
ChunkList mergeAndConvertUpdatesToChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

/// Only used for two level
/// splice aggregatd state of update groups tracked of multiple buckets and convert them to chunk
Chunk spliceAndConvertUpdatesToChunk(
    AggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);
/// Merge many aggregated state of multiple threads, then splice aggregatd state of multiple buckets and convert them to chunk
Chunk mergeAndSpliceAndConvertUpdatesToChunk(
    ManyAggregatedDataVariants & data, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);

/* for EMIT CHANGELOG */
/// Changelog chunk converters are used for changelog emit. They can return a pair of chunks : one
/// for retraction and one for updates. And those 2 chunks are expected to be passed to downstream
/// consecutively otherwise the down stream aggregation result may not be correct or emit incorrect
/// intermediate results. To facilitate the downstream processing, we usually mark the `consecutive`
/// flag bit for these chunks.
/// \return list {retract_chunk, update_chunk}, retract_chunk if not empty, contains retract data
///         because of the current updates; update_chunk if not empty, contains the result for the
///         latest update data
ChunkList convertToChangelogChunks(AggregatedDataVariants & data, const AggregatingTransformParams & params);
ChunkList mergeAndConvertToChangelogChunks(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

inline bool onlyEmitFinalizedWindows(EmitMode mode) noexcept
{
    return mode == EmitMode::Watermark;
}

inline bool onlyEmitUpdates(EmitMode mode) noexcept
{
    return mode >= EmitMode::OnUpdate;
}
}

}
}
