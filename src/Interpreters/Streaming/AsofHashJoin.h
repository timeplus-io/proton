#pragma once

#include <Interpreters/Streaming/HashJoin.h>

namespace DB
{
namespace Streaming
{
class AsofHashJoin final : public HashJoin
{
public:
    AsofHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_);

    HashJoinType type() const override { return HashJoinType::Asof; }

    void joinLeftBlock(Block & block) override;
    void insertRightBlock(Block block) override;

private:
    TypeIndex asof_type;
    ASOFJoinInequality asof_inequality;

    using DataBlock = LightChunkWithTimestamp;
    using BufferedAsofHashData = BufferedHashData<DataBlock, AsofRowRefs<DataBlock>>;
    SERDE std::unique_ptr<BufferedAsofHashData> right_buffered_hash_data;
};

}
}
