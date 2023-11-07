#pragma once

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

struct AggregatingHelper
{
    static Chunk convertWithoutKey(AggregatedDataVariants & data, const AggregatingTransformParams & params);
    static Chunk mergeAndConvertWithoutKey(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

    static Chunk convertSingleLevel(AggregatedDataVariants & data, const AggregatingTransformParams & params);
    static Chunk mergeAndConvertSingleLevel(ManyAggregatedDataVariants & data, const AggregatingTransformParams & params);

    /// Used for emit changelog
    static ChunkPair convertWithoutKeyToChangelog(
        AggregatedDataVariants & data, RetractedDataVariants & retracted_data, const AggregatingTransformParams & params);
    static ChunkPair mergeAndConvertWithoutKeyToChangelog(
        ManyAggregatedDataVariants & data, ManyRetractedDataVariants & retracted_data, const AggregatingTransformParams & params);

    static ChunkPair convertSingleLevelToChangelog(
        AggregatedDataVariants & data, RetractedDataVariants & retracted_data, const AggregatingTransformParams & params);
    static ChunkPair mergeAndConvertSingleLevelToChangelog(
        ManyAggregatedDataVariants & data, ManyRetractedDataVariants & retracted_data, const AggregatingTransformParams & params);
};

}
}
