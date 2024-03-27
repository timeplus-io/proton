#pragma once

#include <Interpreters/IJoin.h>
#include <Interpreters/Streaming/JoinStreamDescription.h>

namespace DB
{
namespace Streaming
{
class IHashJoin : public IJoin
{
public:
    virtual void postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_) = 0;

    virtual void transformHeader(Block & header) = 0;

    /// For non-bidirectional hash join
    virtual void insertRightBlock(Block right_block) = 0;
    virtual void joinLeftBlock(Block & left_block) = 0;

    /// For bidirectional hash join
    /// There are 2 blocks returned : joined block via parameter and retracted block via returned-value if there is
    virtual Block insertLeftBlockAndJoin(Block & left_block) = 0;
    virtual Block insertRightBlockAndJoin(Block & right_block) = 0;

    /// For bidirectional range hash join, there may be multiple joined blocks
    virtual std::vector<Block> insertLeftBlockToRangeBucketsAndJoin(Block left_block) = 0;
    virtual std::vector<Block> insertRightBlockToRangeBucketsAndJoin(Block right_block) = 0;

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

    virtual void serialize(WriteBuffer & wb, VersionType version) const = 0;
    virtual void deserialize(ReadBuffer & rb, VersionType version) = 0;

    virtual void cancel() = 0;
};

using HashJoinPtr = std::shared_ptr<IHashJoin>;
}
}
