#pragma once

#include <Core/BlockWithShard.h>
#include <Interpreters/Streaming/IHashJoin.h>
#include <Interpreters/Streaming/JoinStreamDescription.h>

namespace DB
{
namespace Streaming
{
/**
 * Categorize input blocks according to join keys to buckets for left stream and right stream. And do fine granularity bucket hash join concurrently
 */
class ConcurrentHashJoin final : public IHashJoin
{
public:
    ConcurrentHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        size_t slots_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_);

    ~ConcurrentHashJoin() override = default;

    void rescale(size_t slots_);

    /// Do post initialization
    /// When left stream header is known, init data structure in hash join for left stream
    void postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_) override;

    void transformHeader(Block & header) override;

    /// For non-bidirectional hash join
    void insertRightBlock(Block right_block) override;
    void joinLeftBlock(Block & left_block) override;

    /// For bidirectional hash join
    /// There are 2 blocks returned : joined block via parameter and retracted block via returned-value if there is
    Block insertLeftBlockAndJoin(Block & left_block) override;
    Block insertRightBlockAndJoin(Block & right_block) override;

    /// For bidirectional range hash join, there may be multiple joined blocks
    std::vector<Block> insertLeftBlockToRangeBucketsAndJoin(Block left_block) override;
    std::vector<Block> insertRightBlockToRangeBucketsAndJoin(Block right_block) override;

    bool emitChangeLog() const override { return hash_joins[0]->data->emitChangeLog(); }
    bool bidirectionalHashJoin() const override { return hash_joins[0]->data->bidirectionalHashJoin(); }
    bool rangeBidirectionalHashJoin() const override { return hash_joins[0]->data->rangeBidirectionalHashJoin(); }
    bool leftStreamRequiresBufferingDataToAlign() const override { return hash_joins[0]->data->leftStreamRequiresBufferingDataToAlign(); }
    bool rightStreamRequiresBufferingDataToAlign() const override { return hash_joins[0]->data->rightStreamRequiresBufferingDataToAlign(); }

    /// "Legacy API", use insertRightBlock()
    bool addJoinedBlock(const Block & block, bool check_limits) override;
    /// "Legacy API", use joinLeftBlock()
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;

    const TableJoin & getTableJoin() const override { return *table_join; }
    void checkTypesOfKeys(const Block & block) const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }
    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;
    String metricsString() const override;

    void getKeyColumnPositions(
        std::vector<size_t> & left_key_column_positions_,
        std::vector<size_t> & right_key_column_positions_,
        bool include_asof_key_column) const override
    {
        hash_joins[0]->data->getKeyColumnPositions(left_key_column_positions_, right_key_column_positions_, include_asof_key_column);
    }

    JoinStreamDescriptionPtr leftJoinStreamDescription() const noexcept override
    {
        return hash_joins[0]->data->leftJoinStreamDescription();
    }

    JoinStreamDescriptionPtr rightJoinStreamDescription() const noexcept override
    {
        return hash_joins[0]->data->rightJoinStreamDescription();
    }

    void serialize(WriteBuffer &, VersionType) const override;
    void deserialize(ReadBuffer &, VersionType) override;

    void cancel() override;

private:
    template <bool is_left_block>
    Block insertBlockAndJoin(Block & block);

    template <bool is_left_block>
    std::vector<Block> insertBlockToRangeBucketAndJoin(Block block);

    IColumn::Selector selectDispatchBlock(const std::vector<size_t> & key_column_positions, const Block & from_block);
    BlocksWithShard dispatchBlock(const std::vector<size_t> & key_column_positions, Block && from_block);

    void doSerialize(WriteBuffer &) const;
    void doDeserialize(ReadBuffer &);

private:
    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<IHashJoin> data;
    };

    std::shared_ptr<TableJoin> table_join;
    JoinStreamDescriptionPtr left_join_stream_desc;
    JoinStreamDescriptionPtr right_join_stream_desc;
    size_t slots;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;
    size_t num_used_hash_joins; /// Actual number of used hash joins

    std::vector<size_t> left_key_column_positions;
    std::vector<size_t> right_key_column_positions;

    mutable std::condition_variable serialized;
    mutable std::mutex serialize_mutex;
    mutable std::atomic_uint32_t serialize_requested = 0;

    std::atomic_bool is_cancelled = false;
};

}
}
