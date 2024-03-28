#pragma once

#include <Interpreters/IJoin.h>
#include <Interpreters/Streaming/JoinStreamDescription.h>

namespace DB
{
struct LightChunk;
using LightChunks = std::vector<LightChunk>;

namespace Streaming
{
enum class HashJoinType : uint8_t
{
    /// Dynamic enrichment hash join
    Asof = 1, /// append-only (left/inner) asof join append-only
    Latest = 2, /// append-only (left/inner) latest join append-only
    Changelog = 3, /// append-only (left/inner) all join changelog, for exmaple sources: `versioned_kv, changelog_kv and changelog(...)`

    /// Bidirectional hash join
    BidirectionalAll = 4, /// append-only inner all join append-only
    BidirectionalRange = 5, /// append-only inner all join append-only with on clause `date_diff_within(...)
    BidirectionalChangelog = 6, /// changelog inner all join changelog, for exmaple sources: `versioned_kv, changelog_kv and changelog(...)`
};

class IHashJoin : public IJoin
{
public:
    virtual void postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_) = 0;

    virtual void transformHeader(Block & header) = 0;

    virtual LightChunks insertLeftDataBlockAndJoin(LightChunk && chunk) = 0;
    virtual LightChunks insertRightDataBlockAndJoin(LightChunk && chunk) = 0;

    virtual HashJoinType type() const = 0;
    virtual bool emitChangeLog() const = 0;
    virtual bool bidirectionalHashJoin() const = 0;
    virtual bool rangeBidirectionalHashJoin() const = 0;

    virtual void getKeyColumnPositions(
        std::vector<size_t> & left_key_column_positions,
        std::vector<size_t> & right_key_column_positions,
        bool include_asof_key_column = false) const
        = 0;

    virtual String metricsString() const { return ""; }

    /// Whether hash join algorithm has buffer left/right data to align
    virtual bool leftStreamRequiresBufferingDataToAlign() const = 0;
    virtual bool rightStreamRequiresBufferingDataToAlign() const = 0;

    virtual JoinStreamDescriptionPtr leftJoinStreamDescription() const noexcept = 0;
    virtual JoinStreamDescriptionPtr rightJoinStreamDescription() const noexcept = 0;

    virtual const Block & getOutputHeader() const = 0;

    virtual void serialize(WriteBuffer &, VersionType) const = 0;
    virtual void deserialize(ReadBuffer &, VersionType) = 0;

    virtual void cancel() = 0;
};

using HashJoinPtr = std::shared_ptr<IHashJoin>;
}
}
