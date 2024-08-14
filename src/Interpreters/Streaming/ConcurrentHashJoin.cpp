#include <Interpreters/Streaming/ConcurrentHashJoin.h>
#include <Interpreters/Streaming/HashJoin.h>

#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/WeakHash.h>

#include <memory>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
static UInt32 toPowerOfTwo(UInt32 x)
{
    if (x <= 1)
        return 1;
    return static_cast<UInt32>(1) << (32 - std::countl_zero(x - 1));
}

static UInt32 getSlots(size_t slots)
{
    return toPowerOfTwo(std::min<UInt32>(static_cast<UInt32>(slots), 256));
}

ConcurrentHashJoin::ConcurrentHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    size_t slots_,
    JoinStreamDescriptionPtr left_join_stream_desc_,
    JoinStreamDescriptionPtr right_join_stream_desc_)
    : table_join(table_join_)
    , left_join_stream_desc(std::move(left_join_stream_desc_))
    , right_join_stream_desc(std::move(right_join_stream_desc_))
    , slots(getSlots(slots_))
    , num_used_hash_joins(slots_)
{
    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InternalHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join, left_join_stream_desc, right_join_stream_desc);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }
}

void ConcurrentHashJoin::rescale(size_t slots_)
{
    num_used_hash_joins = slots_;

    auto new_slots = getSlots(slots_);
    if (new_slots == slots)
        return;

    if (slots > new_slots)
    {
        /// scale in
        for (; slots > new_slots; --slots)
            hash_joins.pop_back();
    }
    else
    {
        /// scale up
        for (; slots < new_slots; ++slots)
        {
            auto inner_hash_join = std::make_shared<InternalHashJoin>();
            inner_hash_join->data = std::make_unique<HashJoin>(table_join, left_join_stream_desc, right_join_stream_desc);
            hash_joins.emplace_back(std::move(inner_hash_join));
        }
    }

    assert(slots == new_slots);
    assert(hash_joins.size() == slots);
}

void ConcurrentHashJoin::postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_)
{
    for (auto & hash_join : hash_joins)
        hash_join->data->postInit(left_header, output_header_, join_max_cached_bytes_);

    /// Cache left / right key column positions after post init
    hash_joins[0]->data->getKeyColumnPositions(left_key_column_positions, right_key_column_positions, false);
}

void ConcurrentHashJoin::transformHeader(Block & header)
{
    hash_joins[0]->data->transformHeader(header);
}

void ConcurrentHashJoin::insertRightBlock(Block right_block)
{
    auto dispatched_blocks = dispatchBlock(right_key_column_positions, std::move(right_block));

    size_t blocks_left = dispatched_blocks.size();

    while (blocks_left > 0)
    {
        if (is_cancelled)
            break;

        /// insert blocks into corresponding HashJoin instances
        for (auto & dispatched_block : dispatched_blocks)
        {
            if (dispatched_block.block)
            {
                if (dispatched_block.block.rows() > 0)
                {
                    auto & hash_join = hash_joins[dispatched_block.shard];

                    /// if current hash_join is already processed by another thread, skip it and try later
                    std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                    if (!lock.owns_lock())
                        continue;

                    hash_join->data->insertRightBlock(std::move(dispatched_block.block));
                }
                else
                    dispatched_block.block = {};

                assert(!dispatched_block.block);
                blocks_left--;
            }
        }
    }
}

void ConcurrentHashJoin::joinLeftBlock(Block & left_block)
{
    auto dispatched_blocks = dispatchBlock(right_key_column_positions, std::move(left_block));
    Blocks joined_blocks;
    joined_blocks.reserve(dispatched_blocks.size());

    left_block = {};

    size_t blocks_left = dispatched_blocks.size();
    while (blocks_left > 0)
    {
        if (is_cancelled)
            break;

        for (auto & dispatched_block : dispatched_blocks)
        {
            if (dispatched_block.block)
            {
                if (dispatched_block.block.rows() > 0)
                {
                    auto & hash_join = hash_joins[dispatched_block.shard];

                    /// if current hash_join is already processed by another thread, skip it and try later
                    std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                    if (!lock.owns_lock())
                        continue;

                    hash_join->data->joinLeftBlock(dispatched_block.block);

                    if (dispatched_block.block.rows() > 0)
                        joined_blocks.emplace_back(std::move(dispatched_block.block));
                    else
                        dispatched_block.block = {};
                }
                else
                    dispatched_block.block = {};

                assert(!dispatched_block.block);
                blocks_left--;
            }
        }
    }

    left_block = concatenateBlocks(joined_blocks);
}

template <bool is_left_block>
Block ConcurrentHashJoin::insertBlockAndJoin(Block & block)
{
    const std::vector<size_t> * key_column_positions;
    if constexpr (is_left_block)
        key_column_positions = &left_key_column_positions;
    else
        key_column_positions = &right_key_column_positions;

    auto dispatched_blocks = dispatchBlock(*key_column_positions, std::move(block));

    size_t blocks_left = dispatched_blocks.size();

    Blocks retracted_blocks;
    if (emitChangeLog())
        retracted_blocks.reserve(blocks_left);

    Blocks joined_blocks;
    joined_blocks.reserve(blocks_left);

    while (blocks_left > 0)
    {
        if (is_cancelled)
            break;

        /// insert blocks into corresponding HashJoin instances
        for (auto & dispatched_block : dispatched_blocks)
        {
            if (dispatched_block.block)
            {
                if (dispatched_block.block.rows() > 0)
                {
                    auto & hash_join = hash_joins[dispatched_block.shard];

                    /// if current hash_join is already processed by another thread, skip it and try later
                    std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                    if (!lock.owns_lock())
                        continue;

                    if constexpr (is_left_block)
                    {
                        auto retracted_block = hash_join->data->insertLeftBlockAndJoin(dispatched_block.block);
                        if (retracted_block.rows() > 0)
                            retracted_blocks.emplace_back(std::move(retracted_block));
                    }
                    else
                    {
                        auto retracted_block = hash_join->data->insertRightBlockAndJoin(dispatched_block.block);
                        if (retracted_block.rows() > 0)
                            retracted_blocks.emplace_back(std::move(retracted_block));
                    }

                    if (dispatched_block.block.rows() > 0)
                        joined_blocks.emplace_back(std::move(dispatched_block.block));
                    else
                        dispatched_block.block = {};
                }
                else
                    dispatched_block.block = {};

                assert(!dispatched_block.block);
                blocks_left--;
            }
        }
    }

    block = concatenateBlocks(joined_blocks);
    return concatenateBlocks(retracted_blocks);
}

Block ConcurrentHashJoin::insertLeftBlockAndJoin(Block & left_block)
{
    return insertBlockAndJoin<true>(left_block);
}

Block ConcurrentHashJoin::insertRightBlockAndJoin(Block & right_block)
{
    return insertBlockAndJoin<false>(right_block);
}

template <bool is_left_block>
std::vector<Block> ConcurrentHashJoin::insertBlockToRangeBucketAndJoin(Block block)
{
    const std::vector<size_t> * key_column_positions;
    if constexpr (is_left_block)
        key_column_positions = &left_key_column_positions;
    else
        key_column_positions = &right_key_column_positions;

    auto dispatched_blocks = dispatchBlock(*key_column_positions, std::move(block));

    size_t blocks_left = dispatched_blocks.size();

    std::vector<Block> joined_results;
    joined_results.reserve(blocks_left);

    while (blocks_left > 0)
    {
        if (is_cancelled)
            break;

        /// insert blocks into corresponding HashJoin instances
        for (auto & dispatched_block : dispatched_blocks)
        {
            if (dispatched_block.block)
            {
                if (dispatched_block.block.rows() > 0)
                {
                    auto & hash_join = hash_joins[dispatched_block.shard];

                    /// if current hash_join is already processed by another thread, skip it and try later
                    std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                    if (!lock.owns_lock())
                        continue;

                    if constexpr (is_left_block)
                    {
                        auto joined_blocks = hash_join->data->insertLeftBlockToRangeBucketsAndJoin(std::move(dispatched_block.block));
                        for (auto & joined_block : joined_blocks)
                            joined_results.emplace_back(std::move(joined_block));
                    }
                    else
                    {
                        auto joined_blocks = hash_join->data->insertRightBlockToRangeBucketsAndJoin(std::move(dispatched_block.block));
                        for (auto & joined_block : joined_blocks)
                            joined_results.emplace_back(std::move(joined_block));
                    }
                }
                else
                    dispatched_block.block = {};

                assert(!dispatched_block.block);
                blocks_left--;
            }
        }
    }

    return joined_results;
}

std::vector<Block> ConcurrentHashJoin::insertLeftBlockToRangeBucketsAndJoin(Block left_block)
{
    return insertBlockToRangeBucketAndJoin<true>(std::move(left_block));
}

std::vector<Block> ConcurrentHashJoin::insertRightBlockToRangeBucketsAndJoin(Block right_block)
{
    return insertBlockToRangeBucketAndJoin<false>(std::move(right_block));
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & right_block, bool /*check_limits*/)
{
    insertRightBlock(right_block); /// We have a block copy here
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    joinLeftBlock(block);
}

void ConcurrentHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_joins[0]->data->checkTypesOfKeys(block);
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        if (!hash_join->data->alwaysReturnsEmptySet())
            return false;
    }
    return true;
}

std::shared_ptr<NotJoinedBlocks> ConcurrentHashJoin::getNonJoinedBlocks(
    const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    if (table_join->strictness() == JoinStrictness::Asof || table_join->strictness() == JoinStrictness::Semi
        || !isRightOrFull(table_join->kind()))
    {
        return {};
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Invalid join type. join kind: {}, strictness: {}", table_join->kind(), table_join->strictness());
}

String ConcurrentHashJoin::metricsString() const
{
    WriteBufferFromOwnString wb;
    for (size_t i = 0; const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        wb << "HashJoin-" << i++ << "(" << hash_join->data->metricsString() << ")";
    }
    return wb.str();
}

static ALWAYS_INLINE IColumn::Selector hashToSelector(const WeakHash32 & hash, size_t num_shards)
{
    assert(num_shards > 0 && (num_shards & (num_shards - 1)) == 0);
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        /// Apply intHash64 to mix bits in data.
        /// HashTable internally uses WeakHash32, and we need to get different lower bits not to cause collisions.
        selector[i] = intHash64(data[i]) & (num_shards - 1);
    return selector;
}

IColumn::Selector ConcurrentHashJoin::selectDispatchBlock(const std::vector<size_t> & key_column_positions, const Block & from_block)
{
    size_t num_rows = from_block.rows();
    size_t num_shards = hash_joins.size();

    WeakHash32 hash(num_rows);
    for (const auto & key_column_position : key_column_positions)
    {
        const auto & key_col = from_block.getByPosition(key_column_position).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
        key_col_no_lc->updateWeakHash32(hash);
    }
    return hashToSelector(hash, num_shards);
}

BlocksWithShard ConcurrentHashJoin::dispatchBlock(const std::vector<size_t> & key_column_positions, Block && from_block)
{
    size_t num_shards = hash_joins.size();
    size_t num_cols = from_block.columns();

    IColumn::Selector selector = selectDispatchBlock(key_column_positions, from_block);

    /// Optimized for 1 row block
    if (selector.size() == 1)
        return BlocksWithShard{{std::move(from_block), static_cast<int32_t>(selector[0])}};

    std::vector<std::vector<MutableColumnPtr>> dispatched_columns;
    dispatched_columns.reserve(num_cols);

    for (size_t i = 0; i < num_cols; ++i)
        dispatched_columns.emplace_back(from_block.getByPosition(i).column->scatter(num_shards, selector));

    BlocksWithShard result;
    result.reserve(num_shards);
    for (size_t shard = 0; shard < num_shards; ++shard)
    {
        if (dispatched_columns[0][shard])
        {
            result.emplace_back(from_block.cloneEmpty(), static_cast<int32_t>(shard));
            auto & current_block = result.back().block;

            /// if dispatched column is not null at `shard`
            for (size_t col_pos = 0; col_pos < num_cols; ++col_pos)
                current_block.getByPosition(col_pos).column = std::move(dispatched_columns[col_pos][shard]);
        }
    }

    return result;
}

void ConcurrentHashJoin::serialize(WriteBuffer & wb, VersionType version) const
{
    /// Only last join thread to do serialization
    if (serialize_requested.fetch_add(1) + 1 == num_used_hash_joins)
    {
        if (is_cancelled)
            return;

        DB::writeBoolText(/*serialized*/ true, wb);
        DB::writeVarInt(hash_joins.size(), wb);
        for (const auto & hash_join : hash_joins)
        {
            std::lock_guard lock(hash_join->mutex);
            hash_join->data->serialize(wb, version);
        }

        serialize_requested.store(0, std::memory_order_relaxed);
        serialized.notify_all();
    }
    else
    {
        DB::writeBoolText(/*serialized*/ false, wb);

        /// Condition wait for last join thread to finish the serialization
        std::unique_lock<std::mutex> lk(serialize_mutex);
        if (is_cancelled)
            return;

        serialized.wait(lk);
    }
}

void ConcurrentHashJoin::deserialize(ReadBuffer & rb, VersionType version)
{
    bool is_serialized;
    DB::readBoolText(is_serialized, rb);
    if (!is_serialized)
        return;

    Int64 num_shards;
    DB::readVarInt(num_shards, rb);
    if (num_shards != static_cast<long>(hash_joins.size()))
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover concurrent hash join checkpoint. The concurrent number of hash join are not the same, checkpointed={}, "
            "current={}",
            num_shards,
            hash_joins.size());

    for (auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        hash_join->data->deserialize(rb, version);
    }
}

void ConcurrentHashJoin::cancel()
{
    is_cancelled = true;

    serialize_requested.store(0, std::memory_order_relaxed);

    std::unique_lock<std::mutex> lk(serialize_mutex);
    serialized.notify_all();
}

}
}
