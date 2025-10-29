#include <Interpreters/DirectJoin.h>
#include <Interpreters/castColumn.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

/// right_sample_block can have column alias and after converting the column name back to original column name,
/// original_right_block can has less columns than right_sample_block
/// For example, right_sample_block has columns [name, id, id_alias, created_at], and `id_alias` is an alias of `id` column,
/// this routine basically does
/// 1) Do dedup on the right_sample_block to have unique columns from the right hand side stream
/// 2) Calculate the unique columns (attributes) positions in right_sample_block which is used later to calculate the output block
/// With the above example, after running this routine, the original_right_block and the positions will be
/// 1) [name, id, created_at],
/// 2) [0, 1, 3]
static std::pair<Block, std::vector<size_t>> originalRightBlock(const Block & right_sample_block, const TableJoin & table_join)
{
    std::vector<size_t> right_attributes_positions;
    right_attributes_positions.reserve(right_sample_block.columns());

    Block original_right_block;
    original_right_block.reserve(right_sample_block.columns());

    for (size_t i = 0; const auto & col : right_sample_block)
    {
        auto col_name = table_join.getOriginalName(table_join.restoreRightColumnNameForAlias(col.name));
        if (!original_right_block.has(col_name))
        {
            original_right_block.insert({col.column, col.type, std::move(col_name)});
            right_attributes_positions.push_back(i);
        }
        ++i;
    }
    return {std::move(original_right_block), std::move(right_attributes_positions)};
}

bool DirectKeyValueJoin::needConvertBlockStructure() const
{
    for (const auto & out_sample_col : right_block_to_use)
    {
        const auto * source_col = sample_storage_block.findByName(out_sample_col.name);
        if (source_col && !source_col->type->equals(*out_sample_col.type))
            return true;
    }

    return false;
}

/// Converts `columns` from `source_sample_block` structure to `result_sample_block`.
/// Can select subset of columns and change types.
MutableColumns
DirectKeyValueJoin::convertBlockStructure(MutableColumns && columns, const PaddedPODArray<UInt8> & null_map, [[maybe_unused]] bool negate_null_column) const
{
    MutableColumns result_columns;
    for (const auto & out_sample_col : right_block_to_use)
    {
        /// Some columns from right_block_to_use may not be in sample_storage_block,
        /// e.g. if they will be calculated later based on joined columns
        const auto * source_col = sample_storage_block.findByName(out_sample_col.name);
        if (!source_col)
            continue;

        auto i = sample_storage_block.getPositionByName(out_sample_col.name);
        if (columns[i] == nullptr)
        {
            throw DB::Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Can't find column '{}'", out_sample_col.name);
        }

        if (!source_col->type->equals(*out_sample_col.type))
        {
            ColumnWithTypeAndName col = *source_col; /// Use source type
            col.column = std::move(columns[i]);
            result_columns.push_back(IColumn::mutate(castColumnAccurate(col, out_sample_col.type)));
        }
        else
        {
            result_columns.push_back(std::move(columns[i]));
        }
        columns[i] = nullptr;

        if (result_columns.back()->isNullable())
        {
            assert_cast<ColumnNullable *>(result_columns.back().get())->applyNegatedNullMap(null_map);
        }
    }
    return result_columns;
}

DirectKeyValueJoin::DirectKeyValueJoin(
    std::shared_ptr<TableJoin> table_join_,
    const Block & right_sample_block_,
    std::shared_ptr<const IKeyValueEntity> storage_,
    std::pair<Block, std::vector<size_t>> right_sample_block_with_storage_column_names_,
    size_t index_id_)
    : table_join(std::move(table_join_))
    , storage(storage_)
    , right_sample_block(right_sample_block_)
    , index_id(index_id_)
    , right_block_to_use(std::move(right_sample_block_with_storage_column_names_.first))
    , attribute_names(right_block_to_use.getNames())
    , attribute_positions(std::move(right_sample_block_with_storage_column_names_.second))
    , sample_storage_block(storage->getSampleBlock(attribute_names))
    , logger(getLogger(fmt::format("Direct{}Join", storage->keyValueType())))
{
    auto index_keys = storage->getIndexKey(index_id_);
    index_column_map.reserve(index_keys.size());
    for (size_t i = 0; auto & k : index_keys)
        index_column_map.emplace(std::move(k), i++);

    if (!table_join->oneDisjunct())
        throw DB::Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Multiple clause join is not supported by direct JOIN");

    bool allowed_inner = isInner(table_join->kind())
        && (table_join->strictness() == JoinStrictness::All || table_join->strictness() == JoinStrictness::Any
            || table_join->strictness() != JoinStrictness::RightAny);

    bool allowed_left = isLeft(table_join->kind())
        && (table_join->strictness() == JoinStrictness::Any || table_join->strictness() == JoinStrictness::All
            || table_join->strictness() == JoinStrictness::Semi || table_join->strictness() == JoinStrictness::Anti);
    if (!allowed_inner && !allowed_left)
        throw DB::Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Strictness {} and kind {} is not supported by direct JOIN",
            table_join->strictness(),
            table_join->kind());

    /// Join key order : JOIN ... ON left.key2 = key_value.key2 AND left.key1 = key_value.key1; (key2, key1)
    /// Storage primary key: PRIMARY KEY (key1, key2)
    std::vector<String> origin_right_join_key_cols;

    const auto & join_clause = table_join->getClauses()[0];

    if (join_clause.key_names_restored.empty())
    {
        const auto & right_join_key_cols = join_clause.key_names_right;
        origin_right_join_key_cols.reserve(right_join_key_cols.size());
        for (const auto & key_name : right_join_key_cols)
            origin_right_join_key_cols.push_back(table_join->getOriginalName(key_name));
    }
    else
    {
        const auto & right_join_key_cols = join_clause.key_names_restored;
        origin_right_join_key_cols.reserve(right_join_key_cols.size());
        for (const auto & key_name : right_join_key_cols)
            origin_right_join_key_cols.push_back(table_join->getOriginalName(table_join->restoreRightColumnNameForAlias(key_name)));
    }

    absl::flat_hash_map<std::string_view, size_t> join_key_index;
    join_key_index.reserve(origin_right_join_key_cols.size());

    for (size_t idx = 0; const auto & key_col : origin_right_join_key_cols)
    {
        auto [_, inserted] = join_key_index.emplace(key_col, idx);
        if (!inserted)
            /// Duplicate join key column
            throw DB::Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Found duplicate join key column");

        ++idx;
    }

    const auto & storage_primary_keys = storage->getIndexKey(index_id);
    primary_key_indexes.reserve(origin_right_join_key_cols.size());
    for (size_t i = 0; i < origin_right_join_key_cols.size(); ++i)
    {
        auto iter = join_key_index.find(storage_primary_keys[i]);
        chassert(iter != join_key_index.end());
        primary_key_indexes.push_back(iter->second);
    }

    is_semi_join = table_join->strictness() == JoinStrictness::Semi;
    is_anti_join = table_join->strictness() == JoinStrictness::Anti;
    filter_non_joined_rows = isInner(table_join->kind()) || (isLeft(table_join->kind()) && (is_semi_join || is_anti_join));

    /// Calculate if we will need convert block structure from storage block to resulting block
    convert_block_structure = needConvertBlockStructure();

    LOG_INFO(
        logger,
        "Use kv direct join, is_semi_join={} is_anti_join={} filter_non_joined_rows={} convert_block_structure={}",
        is_semi_join,
        is_anti_join,
        filter_non_joined_rows,
        convert_block_structure);
}

DirectKeyValueJoin::DirectKeyValueJoin(
    std::shared_ptr<TableJoin> table_join_,
    const Block & right_sample_block_,
    std::shared_ptr<const IKeyValueEntity> storage_,
    size_t index_id_)
    : DirectKeyValueJoin(
          table_join_, right_sample_block_, std::move(storage_), originalRightBlock(right_sample_block_, *table_join_), index_id_)
{
}

bool DirectKeyValueJoin::addJoinedBlock(const Block &, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code reached");
}

void DirectKeyValueJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_sample_block, onexpr.key_names_right);
}

void DirectKeyValueJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> &)
{
    size_t num_keys = block.rows();
    if (num_keys == 0 && empty_result_block)
    {
        /// Copy over empty result block
        block = *empty_result_block;
        return;
    }

    std::vector<ColumnWithTypeAndName> left_join_key_columns;
    left_join_key_columns.reserve(primary_key_indexes.size());

    for (const auto & left_key_name : table_join->getOnlyClause().key_names_left)
    {
        left_join_key_columns.push_back(block.getByName(left_key_name));
        if (!left_join_key_columns.back().column)
            return;
    }

    Stopwatch stopwatch;
    SCOPE_EXIT({
        if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms >= 200)
            LOG_INFO(logger, "Looking up {} keys, {} was found and took {}ms", num_keys, block.rows(), elapsed_ms);
    });

    NullMap null_map;
    Chunk joined_chunk;
    if (storage->supportMultipleValuesPerKey())
        joinBlockAll(block, left_join_key_columns, joined_chunk, null_map);
    else
        joinBlockOne(block, left_join_key_columns, joined_chunk, null_map);

    if (is_anti_join)
        createChunkForAntiJoin(block, left_join_key_columns, joined_chunk, null_map);

    chassert(joined_chunk.rows() == block.rows());
    chassert(joined_chunk.rows() == null_map.size());

    auto result_columns = joined_chunk.mutateColumns();

    /// Expected right block may differ from structure in storage, because of `join_use_nulls`
    /// or we just select not all joined attributes
    if (convert_block_structure)
        result_columns = convertBlockStructure(std::move(result_columns), null_map, /*negate_null_column=*/false);

    /// Fix the joined column name by using right_sample_block
    for (size_t i = 0, result_column_size = result_columns.size(); i < result_column_size; ++i)
    {
        ColumnWithTypeAndName col = right_sample_block.getByPosition(attribute_positions[i]);
        col.column = std::move(result_columns[i]);
        block.insert(std::move(col));
    }

    if (!empty_result_block)
        empty_result_block.emplace(block.cloneEmpty());
}

void DirectKeyValueJoin::joinBlockOne(
    Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map)
{
    const bool add_defaults_if_missing = !filter_non_joined_rows;
    joined_chunk = storage->getByKeys(left_join_key_columns, null_map, attribute_names, primary_key_indexes, add_defaults_if_missing);
    assert(block.rows() == null_map.size());

    /// Adjust block rows (left table)
    if (filter_non_joined_rows)
    {
        MutableColumns dst_columns = block.mutateColumns();
        if (!is_anti_join)
        {
            for (auto & col : dst_columns)
                col = IColumn::mutate(col->filter(null_map, -1));
        }
        else
        {
            NullMap anti_null_map(null_map.size());
            for (size_t i = 0; i < null_map.size(); ++i)
                anti_null_map[i] = static_cast<UInt8>(null_map[i] == 0);

            for (auto & col : dst_columns)
                col = IColumn::mutate(col->filter(anti_null_map, -1));
        }
        block.setColumns(std::move(dst_columns));
    }

    /// After using null_map to filter left block, adjust it to match joined_chunk(for inner join).
    if (!add_defaults_if_missing)
        null_map.resize_fill(joined_chunk.rows(), 1);
}

void DirectKeyValueJoin::joinBlockAll(
    Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map)
{
    const auto num_keys = block.rows();
    PaddedPODArray<size_t> out_values;
    joined_chunk = storage->getAllByKeys(left_join_key_columns, out_values, attribute_names, primary_key_indexes, !filter_non_joined_rows);
    chassert(out_values.size() == num_keys);
    handleOneToManyJoinResult(block, joined_chunk, out_values, null_map);
}

void DirectKeyValueJoin::handleOneToManyJoinResult(
    Block & block, Chunk & joined_chunk, PaddedPODArray<size_t> & out_matched_rows, NullMap & null_map)
{
    const auto num_keys = block.rows();
    const bool add_defaults_if_missing = !filter_non_joined_rows;

    if (!is_anti_join)
    {
        /// Expand block by out_matched_rows
        const auto & src_columns = block.getColumnsWithTypeAndName();
        auto dst_columns = block.cloneEmptyColumns();
        for (size_t col = 0, num_cols = dst_columns.size(); col < num_cols; ++col)
        {
            const auto & col_src = src_columns[col].column;
            auto & col_dst = dst_columns[col];
            chassert(col_src->size() == num_keys);
            for (size_t row = 0; row < num_keys; ++row)
            {
                if (out_matched_rows[row] > 0)
                    col_dst->insertManyFrom(*col_src, row, out_matched_rows[row]);
                else if (add_defaults_if_missing)
                    col_dst->insertFrom(*col_src, row);

                if (add_defaults_if_missing && col == 0)
                {
                    /// Create null_map from out_matched_rows
                    for (size_t i = 0; i < out_matched_rows[row]; ++i)
                        null_map.push_back(1);
                    if (out_matched_rows[row] == 0)
                        null_map.push_back(0);
                }
            }
        }
        block.setColumns(std::move(dst_columns));

        if (!add_defaults_if_missing)
            null_map.resize_fill(joined_chunk.rows(), 1);

        chassert(null_map.size() == joined_chunk.rows());
    }
    else
    {
        /// Remove rows which have any match.
        IColumn::Filter filter(out_matched_rows.size());
        for (size_t i = 0; i < out_matched_rows.size(); ++i)
            filter[i] = static_cast<UInt8>(out_matched_rows[i] == 0);

        MutableColumns dst_columns = block.mutateColumns();
        for (auto & col : dst_columns)
            col = IColumn::mutate(col->filter(filter, -1));
        block.setColumns(std::move(dst_columns));
    }
}

void DirectKeyValueJoin::createChunkForAntiJoin(
    const Block & block, const ColumnsWithTypeAndName & left_join_key_columns, Chunk & joined_chunk, NullMap & null_map)
{
    const auto rows = block.rows();
    const auto & header = right_block_to_use.getColumnsWithTypeAndName();
    Columns columns;
    columns.reserve(header.size());
    for (const auto & col : header)
    {
        if (auto iter = index_column_map.find(col.name); iter != index_column_map.end() && iter->second < primary_key_indexes.size())
        {
            /// shallow copy join key column
            const auto & left_name = left_join_key_columns[primary_key_indexes[iter->second]].name;
            columns.push_back(block.getByName(left_name).column);
        }
        else
        {
            columns.push_back(col.column->cloneEmpty()->cloneResized(rows));
        }
    }
    joined_chunk.setColumns(std::move(columns), rows);
    null_map.resize_fill(rows, 1);
}
}
