#pragma once
#include <Interpreters/Streaming/HashJoin/HashJoin.h>
#include <Interpreters/Streaming/HashJoin/joinCommon.h>

#include <Columns/ColumnNullable.h>

namespace DB::Streaming
{
class AddedColumns
{
public:
    struct TypeAndName
    {
        DataTypePtr type;
        String name;
        String qualified_name;

        TypeAndName(DataTypePtr type_, const String & name_, const String & qualified_name_)
            : type(type_), name(name_), qualified_name(qualified_name_)
        {
        }
    };

    AddedColumns(
        Block & block,
        const Block & block_with_columns_to_add,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        const RangeAsofJoinContext & range_join_ctx_,
        bool is_asof_join,
        bool is_left_block_ = true)
        : join_on_keys(std::move(join_on_keys_))
        , rows_to_add(block.rows())
        , range_join_ctx(range_join_ctx_)
        , is_left_block(is_left_block_)
        , asof_type(join.getAsofType())
        , asof_inequality(join.getAsofInequality())
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();

        if (is_asof_join)
            /// Asof join has one more column to add (asof join key)
            ++num_columns_to_add;

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        if (is_left_block)
        {
            for (const auto & src_column : block_with_columns_to_add)
            {
                /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
                /// because it uses not qualified right block column names
                auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
                /// Don't insert column if it's in left block
                if (!block.has(qualified_name))
                    addColumn(src_column, qualified_name);
            }

            if (is_asof_join)
            {
                chassert(join_on_keys.size() == 1);
                const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
                addColumn(right_asof_column, right_asof_column.name);
                asof_key = join_on_keys[0].key_columns.back();
            }

            nullable_column_ptrs.resize(right_indexes.size(), nullptr);
            for (size_t j = 0; j < right_indexes.size(); ++j)
            {
                /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
                const auto & saved_column = saved_block_sample.getByPosition(right_indexes[j]).column;
                if (columns[j]->isNullable() && !saved_column->isNullable())
                    nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
            }
        }
        else
        {
            /// This is a right block which means it is bi-directional hash join and
            /// it is right block join left hash table, we don't need rename column name.
            /// So add left column directly
            /// Actually this may not be the case, right block may have same column name as in
            /// left block, we need rename source right block
            for (size_t pos = 0; auto & col : block)
            {
                auto qualified_name = join.getTableJoin().renamedRightColumnName(col.name);
                if (qualified_name != col.name)
                    block.renameColumn(qualified_name, pos);

                ++pos;
            }

            for (const auto & src_column : block_with_columns_to_add)
            {
                if (!block.has(src_column.name))
                    addColumn(src_column, src_column.name);
            }

            if (is_asof_join)
            {
                chassert(join_on_keys.size() == 1);
                const ColumnWithTypeAndName & left_asof_column = join.leftAsofKeyColumn();
                addColumn(left_asof_column, left_asof_column.name);
                asof_key = join_on_keys[0].key_columns.back();
            }
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }

    template <bool has_defaults, typename DataBlock>
    void appendFromBlock(const DataBlock & block, size_t row_num)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

        const auto & source_columns = block.getColumns();
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            columns[j]->insertFrom(*source_columns[right_indexes[j]], row_num);
    }

    template <bool has_defaults>
    void appendFromRow(const Row & row)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            columns[j]->insert(row[right_indexes[j]]);
    }

    void appendDefaultRow() { ++lazy_defaults_count; }
    void appendDefaultRows(size_t count) { lazy_defaults_count += count; }

    void applyLazyDefaults()
    {
        if (lazy_defaults_count)
        {
            for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
                JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
            lazy_defaults_count = 0;
        }
    }

    TypeIndex asofType() const { return *asof_type; }
    ASOFJoinInequality asofInequality() const { return asof_inequality; }
    const IColumn & asofKey() const { return *asof_key; }

    std::vector<JoinOnKeyColumns> join_on_keys;

    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;

    const RangeAsofJoinContext & range_join_ctx;
    bool is_left_block;

private:
    std::vector<TypeAndName> type_name;
    MutableColumns columns;
    std::vector<ColumnNullable *> nullable_column_ptrs;

    std::vector<size_t> right_indexes;
    size_t lazy_defaults_count = 0;
    /// for ASOF or range join
    std::optional<TypeIndex> asof_type;
    ASOFJoinInequality asof_inequality;
    const IColumn * asof_key = nullptr;

    void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name, qualified_name);
    }
};
}
