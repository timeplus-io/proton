#pragma once

#include <Interpreters/Streaming/HashJoin.h>

namespace DB
{
namespace Streaming
{
AsofHashJoin::AsofHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    JoinStreamDescriptionPtr left_join_stream_desc_,
    JoinStreamDescriptionPtr right_join_stream_desc_)
    : HashJoin(table_join_, left_join_stream_desc_, right_join_stream_desc_)
    , asof_type(*table_join->getAsofType())
    , asof_inequality(table_join->getAsofInequality())
{
    /// last key is asof column, not use as a hash key
    hash_key_sizes = key_sizes;
    hash_key_sizes.back();
}

void AsofHashJoin::joinLeftBlock(Block & block)
{
    doJoinBlockWithHashTable<true>(block, hash_blocks);
}

void AsofHashJoin::insertRightBlock(Block block)
{
    /// FIXME, there are quite some block copies
    /// FIXME, all_key_columns shall hold shared_ptr to columns instead of raw ptr
    /// then we can update `source_block` in place
    /// key columns are from source `block`
    ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(block, table_join->getAllNames(JoinTableSide::Right));

    /// We have copy of source `block` to `block_to_save` after prepare, so `block_to_save` is good to get moved to the buffered stream data
    Block block_to_save = prepareBlockToSave(block, right_data.buffered_data->sample_block);

    /// FIXME, multiple disjuncts OR clause
    ColumnRawPtrs key_columns;
    const Names & key_names = table_join->getClauses().front().key_names_right;
    key_columns.reserve(key_names.size());
    for (const auto & name : key_names)
        key_columns.push_back(all_key_columns[name]);

    auto asof_column = key_columns.back();
    key_columns.pop_back();

    /// Add `block_to_save` to target stream data
    /// Note `block_to_save` may be empty for cases in which the query doesn't care other non-key columns.
    /// For example, SELECT count() FROM stream_a JOIN stream_b ON i=ii;
    auto rows = block_to_save.rows();
    auto start_row = right_buffered_hash_data->blocks.pushBackOrConcat(std::move(block_to_save));
    auto row_ref_handler = [&](AsofRowRef & row_ref, bool inserted, size_t original_row, size_t row) {
        AsofRowRef * row_ref_ptr = &row_ref;
        if (inserted)
            row_ref_ptr = new (row_ref_ptr) AsofRowRef(asof_type);

        row_ref_ptr->insert(
            asof_type,
            asof_column,
            &(right_buffered_hash_data->blocks),
            original_row,
            row,
            asof_inequality,
            rightJoinStreamDescription()->keep_versions);
    };

    right_buffered_hash_data->map.insert(std::move(block_to_save), key_columns, hash_key_sizes, rows, right_buffered_hash_data->pool, std::move(row_ref_handler));

    checkLimits();
}

void AsofHashJoin::checkLimits() const
{
    auto current_total_bytes = right_buffered_hash_data->totalBufferedBytes();
    if (current_total_bytes >= join_max_cached_bytes)
        throw Exception(
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED,
            "Streaming asof join's memory reaches max size: {}, current total: {}, right: {}",
            join_max_cached_bytes,
            current_total_bytes,
            buffered_hash_data->getMetricsString());
}
}
}
