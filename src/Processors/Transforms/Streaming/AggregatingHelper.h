#pragma once

#include <Columns/IColumn.h>
#include <Core/Streaming/Watermark.h>
#include <base/defines.h>
#include <base/types.h>

#include <list>
#include <vector>

namespace DB
{
class Block;
class Chunk;

using KeyColumns = std::list<Columns>;
using ManyKeyColumns = std::vector<KeyColumns>;

namespace Streaming
{
struct AggregatingTransformParams;
struct IAggregatedDataVariants;
using IAggregatedDataVariantsPtr = std::shared_ptr<IAggregatedDataVariants>;
using ManyIAggregatedDataVariants = std::vector<IAggregatedDataVariantsPtr>;

using ChunkPair = std::pair<Chunk, Chunk>;
using ChunkList = std::list<Chunk>;

namespace AggregatingHelper
{
/// Convert aggregated state to chunk
ChunkList convertToChunks(IAggregatedDataVariants & data, const AggregatingTransformParams & params);
/// Merge many aggregated state and convert them to chunk
ChunkList mergeAndConvertToChunks(ManyIAggregatedDataVariants & data, const AggregatingTransformParams & params);

/// Only used for two level
/// splice aggregated state of multiple buckets and convert them to chunk
Chunk spliceAndConvertToChunk(
    IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);
/// Merge many aggregated state of multiple threads, then splice aggregated state of multiple buckets and convert them to chunk
Chunk mergeAndSpliceAndConvertToChunk(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);

/// for EMIT ON UPDATE
/// Convert aggregated state of update groups tracked to chunk
ChunkList
convertUpdatesToChunks(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, KeyColumns key_columns = {});
/// Merge many aggregated state and convert them to chunk
ChunkList mergeAndConvertUpdatesToChunks(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, ManyKeyColumns many_key_columns = {});

/// Only used for two level time bucket
/// splice aggregated state of update groups tracked of multiple buckets and convert them to chunk
Chunk spliceAndConvertUpdatesToChunk(
    IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);
/// Merge many aggregated state of multiple threads, then splice aggregated state of multiple buckets and convert them to chunk
Chunk mergeAndSpliceAndConvertUpdatesToChunk(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, const std::vector<Int64> & buckets);

/// for EMIT CHANGELOG
/// Changelog chunk converters are used for changelog emit. They can return a pair of chunks : one
/// for retraction and one for updates. And those 2 chunks are expected to be passed to downstream
/// consecutively otherwise the down stream aggregation result may not be correct or emit incorrect
/// intermediate results. To facilitate the downstream processing, we usually mark the `consecutive`
/// flag bit for these chunks.
/// \return list {retract_chunk, update_chunk}, retract_chunk if not empty, contains retract data
///         because of the current updates; update_chunk if not empty, contains the result for the
///         latest update data
ChunkList
convertToChangelogChunks(IAggregatedDataVariants & data_variants, const AggregatingTransformParams & params, KeyColumns key_columns = {});
ChunkList mergeAndConvertToChangelogChunks(
    ManyIAggregatedDataVariants & many_data_variants, const AggregatingTransformParams & params, ManyKeyColumns many_key_columns = {});

inline bool onlyEmitFinalizedWindows(EmitMode mode) noexcept
{
    return mode == EmitMode::AfterWindowClose;
}

inline bool onlyEmitUpdates(EmitMode mode) noexcept
{
    return mode >= EmitMode::OnUpdate;
}

}

}

}
