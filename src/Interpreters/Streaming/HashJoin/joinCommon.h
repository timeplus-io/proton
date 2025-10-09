#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>

namespace DB::Streaming
{
template <Kind KIND, Strictness STRICTNESS, template <Kind, Strictness> typename HashMapGetter>
struct JoinFeatures
{
    static constexpr bool is_multi_join = STRICTNESS == Strictness::Multiple;
    static constexpr bool is_latest_join = STRICTNESS == Strictness::Latest;
    static constexpr bool is_all_join = STRICTNESS == Strictness::All;
    static constexpr bool is_asof_join = STRICTNESS == Strictness::Asof;
    static constexpr bool is_range_join = STRICTNESS == Strictness::Range;

    static constexpr bool left = KIND == Kind::Left;
    static constexpr bool right = KIND == Kind::Right;
    static constexpr bool inner = KIND == Kind::Inner;
    static constexpr bool full = KIND == Kind::Full;

    static constexpr bool need_replication = is_all_join || (is_latest_join && right) || is_range_join || is_multi_join;
    static constexpr bool need_filter = !need_replication && (inner || right);
    static constexpr bool add_missing = left || full;

    static constexpr bool need_flags = HashMapGetter<KIND, STRICTNESS>::flagged;
};


struct JoinOnKeyColumns
{
    Names key_names;

    Columns materialized_keys_holder;
    ColumnRawPtrs key_columns;

    ConstNullMapPtr null_map;
    ColumnPtr null_map_holder;

    /// Only rows where mask == true can be joined
    JoinCommon::JoinMask join_mask_column;

    std::vector<size_t> key_sizes;

    explicit JoinOnKeyColumns(
        const Block & block, const Names & key_names_, const String & cond_column_name, const std::vector<size_t> & key_sizes_)
        : key_names(key_names_)
        , materialized_keys_holder(JoinCommon::materializeColumns(
              block, key_names)) /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
        , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
        , null_map(nullptr)
        , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
        , join_mask_column(JoinCommon::getColumnAsMask(block, cond_column_name))
        , key_sizes(key_sizes_)
    {
    }

    bool isRowFiltered(size_t i) const { return join_mask_column.isRowFiltered(i); }
};

/** Since we do not store right key columns,
  * this function is used to copy left key columns to right key columns.
  * If the user requests some right columns, we just copy left key columns to right, since they are equal.
  * Example: SELECT t1.key, t2.key FROM t1 FULL JOIN t2 ON t1.key = t2.key;
  * In that case for matched rows in t2.key we will use values from t1.key.
  * However, in some cases we might need to adjust the type of column, e.g. t1.key :: LowCardinality(String) and t2.key :: String
  * Also, the nullability of the column might be different.
  * Returns the right column after with necessary adjustments.
  */
ColumnWithTypeAndName copyLeftKeyColumnToRight(
    const DataTypePtr & right_key_type,
    const String & renamed_right_column,
    const ColumnWithTypeAndName & left_column,
    const IColumn::Filter * null_map_filter = nullptr);

}
