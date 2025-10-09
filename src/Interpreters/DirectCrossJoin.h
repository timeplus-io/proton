#pragma once

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Storages/IStorage.h>

namespace DB
{

class DirectCrossJoin : public IJoin
{
public:
    DirectCrossJoin(
        std::shared_ptr<TableJoin> table_join_,
        const Block & right_sample_block_,
        std::shared_ptr<IStorage> storage_,
        bool cache_enabled_,
        Int64 default_cache_expire_sec_);

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addJoinedBlock(const Block &, bool) override;
    void checkTypesOfKeys(const Block &) const override;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> &) override;

    size_t getTotalRowCount() const override { return 0; }

    size_t getTotalByteCount() const override { return 0; }

    bool alwaysReturnsEmptySet() const override { return false; }

    bool isFilled() const override { return true; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

protected:
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<IStorage> storage;
    Block right_sample_block;
    Names required_right_names;
    std::vector<size_t> required_right_positions;

    std::optional<Block> empty_result_block;

    bool cache_enabled;
    Int64 default_cache_expire_sec;
    IStorage::SnapshotDataWithExpiration cached_right_data;

    LoggerPtr logger;
};

}
