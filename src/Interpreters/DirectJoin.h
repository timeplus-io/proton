#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/TableJoin.h>
#include <Common/ProtonCommon.h>

namespace DB
{

class DirectKeyValueJoin : public IJoin
{
public:
    DirectKeyValueJoin(
        std::shared_ptr<TableJoin> table_join_,
        const Block & right_sample_block_,
        std::shared_ptr<const IKeyValueEntity> storage_,
        size_t index_id_ = ProtonConsts::PRIMARY_KEY_INDEX_ID);

    DirectKeyValueJoin(
        std::shared_ptr<TableJoin> table_join_,
        const Block & right_sample_block_,
        std::shared_ptr<const IKeyValueEntity> storage_,
        std::pair<Block, std::vector<size_t>> right_sample_block_with_storage_column_names_,
        size_t index_id_ = ProtonConsts::PRIMARY_KEY_INDEX_ID);

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
    bool needConvertBlockStructure() const;
    MutableColumns convertBlockStructure(MutableColumns && columns, const PaddedPODArray<UInt8> & null_map, bool negate_null_column) const;

protected:
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<const IKeyValueEntity> storage;
    Block right_sample_block;
    // Block right_sample_block_with_storage_column_names;
    Block sample_block_with_columns_to_add;

    size_t index_id;
    NameToIndexMap index_column_map;

    Block right_block_to_use;
    Names attribute_names; /// column names defined
    std::vector<size_t> attribute_positions; /// column positions from right_sample_block

    Block sample_storage_block; /// For required attribute_names

    /// Mapping join key column index to storage primary key index
    std::vector<size_t> primary_key_indexes;

    bool is_semi_join;
    bool is_anti_join;
    bool filter_non_joined_rows;
    bool convert_block_structure;

    std::optional<Block> empty_result_block;

    LoggerPtr logger;

private:
    /// proton: starts
    /// join key have at most one row in the right table
    void joinBlockOne(Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map);
    /// join key may have multiple rows in the right table
    void joinBlockAll(Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map);

    /// Common steps to expand block and create null_map by out_matched_rows of the one-to-many join.
    void handleOneToManyJoinResult(Block & block, Chunk & joined_chunk, PaddedPODArray<size_t> & out_matched_rows, NullMap & null_map);
    /// Create joined_chunk and its null_map for anti join
    void createChunkForAntiJoin(
        const Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map);
    /// proton: ends
};

}
