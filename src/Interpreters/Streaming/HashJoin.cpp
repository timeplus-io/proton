#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/joinDispatch.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Interpreters/Streaming/ChooseHashMethod.h>
#include <Interpreters/TableJoin.h>
#include <Common/ColumnsHashing.h>
#include <Common/ProtonCommon.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <any>
#include <limits>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NO_SUCH_COLUMN_IN_STREAM;
extern const int INCOMPATIBLE_TYPE_OF_JOIN;
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int LOGICAL_ERROR;
extern const int SYNTAX_ERROR;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int RECOVER_CHECKPOINT_FAILED;
extern const int INTERNAL_ERROR;
extern const int UNSUPPORTED;
}

namespace Streaming
{
namespace
{

void ALWAYS_INLINE insertRow(Block & target_block, size_t row_num, const Columns & source_data)
{
    assert(source_data.size() == target_block.columns());
    assert(row_num < source_data.at(0)->size());
    for (size_t col_pos = 0; const auto & col : source_data)
        target_block.getByPosition(col_pos++).column->assumeMutable()->insertFrom(*col, row_num);
}

int ALWAYS_INLINE compareAt(
    size_t lhs_row, size_t rhs_row, const Columns & lhs_columns, const Columns & rhs_columns, const std::vector<size_t> & skip_columns)
{
    assert(lhs_columns.size() == rhs_columns.size());

    for (size_t i = 0, num_columns = lhs_columns.size(); i < num_columns; ++i)
    {
        if (std::find(skip_columns.begin(), skip_columns.end(), i) != skip_columns.end())
            continue;

        const auto & lhs_col = lhs_columns[i];
        const auto & rhs_col = rhs_columns[i];

        if (auto r = lhs_col->compareAt(lhs_row, rhs_row, *rhs_col, -1); r != 0)
            return r;
    }

    return 0;
}

template <typename KeyGetter, bool is_asof_join>
KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

struct JoinOnKeyColumns
{
    Names key_names;

    Columns materialized_keys_holder;
    ColumnRawPtrs key_columns;

    ConstNullMapPtr null_map;
    ColumnPtr null_map_holder;

    /// Only rows where mask == true can be joined
    JoinCommon::JoinMask join_mask_column;

    Sizes key_sizes;

    explicit JoinOnKeyColumns(const Block & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_)
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
        const Block & block_with_columns_to_add,
        Block & block,
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
                assert(join_on_keys.size() == 1);
                const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
                addColumn(right_asof_column, right_asof_column.name);
                asof_key = join_on_keys[0].key_columns.back();
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
                assert(join_on_keys.size() == 1);
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

        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            columns[j]->insertFrom(*block.getColumns()[right_indexes[j]], row_num);
    }

    void appendDefaultRow() { ++lazy_defaults_count; }

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

    /// proton : starts
    const RangeAsofJoinContext & range_join_ctx;
    bool is_left_block;
    /// proton : ends

private:
    std::vector<TypeAndName> type_name;
    MutableColumns columns;
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

template <Kind KIND, Strictness STRICTNESS>
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

    static constexpr bool need_replication = is_all_join || (is_latest_join && right) || is_range_join || is_multi_join;
    static constexpr bool need_filter = !need_replication && (inner || right);
    static constexpr bool add_missing = left;

    static constexpr bool need_flags = MapGetter<KIND, STRICTNESS>::flagged;
};

template <typename Map, bool add_missing>
void addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added_columns,
    IColumn::Offset & current_offset,
    uint32_t row_num_in_left_block)
{
    if constexpr (add_missing)
        added_columns.applyLazyDefaults();

    for (auto it = mapped.begin(); it.ok(); ++it)
    {
        added_columns.appendFromBlock<false>(*it->block, it->row_num);
        ++current_offset;
    }
};

template <bool add_missing, bool need_offset>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]])
{
    if constexpr (need_filter)
        filter[pos] = 1;
}

/// Joins columns which indexes are present in the corresponding index using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map, bool multiple_disjuncts>
NO_INLINE IColumn::Filter
joinColumns(std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    Arena pool;

    if constexpr (jf.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;

    for (uint32_t i = 0; i < rows; ++i)
    {
        bool right_row_found = false;
        bool null_element_found = false;

        for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
            if constexpr (has_null_map)
            {
                if (join_keys.null_map && (*join_keys.null_map)[i])
                {
                    null_element_found = true;
                    continue;
                }
            }

            bool row_acceptable = !join_keys.isRowFiltered(i);
            using FindResult = typename KeyGetter::FindResult;
            auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), i, pool) : FindResult();

            if (find_result.isFound())
            {
                right_row_found = true;
                auto & mapped = find_result.getMapped();
                if constexpr (jf.is_range_join)
                {
                    TypeIndex asof_type = added_columns.asofType();
                    const IColumn & asof_key = added_columns.asofKey();

                    if (auto row_refs = mapped.findRange(asof_type, added_columns.range_join_ctx, asof_key, i, added_columns.is_left_block);
                        !row_refs.empty())
                    {
                        setUsed<need_filter>(filter, i);

                        for (auto & row_ref : row_refs)
                        {
                            added_columns.appendFromBlock<jf.add_missing>(*row_ref.block, row_ref.row_num);
                            ++current_offset;
                        }
                    }
                    else
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                }
                else if constexpr (jf.is_asof_join)
                {
                    TypeIndex asof_type = added_columns.asofType();
                    ASOFJoinInequality asof_inequality = added_columns.asofInequality();
                    const IColumn & asof_key = added_columns.asofKey();

                    if (const auto * found = mapped.findAsof(asof_type, asof_inequality, asof_key, i))
                    {
                        setUsed<need_filter>(filter, i);
                        added_columns.appendFromBlock<jf.add_missing>(found->block_iter->block, found->row_num);
                    }
                    else
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                }
                else if constexpr (jf.is_all_join)
                {
                    setUsed<need_filter>(filter, i);
                    addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, i);
                }
                else if constexpr (jf.is_multi_join)
                {
                    if (!mapped->rows.empty())
                    {
                        setUsed<need_filter>(filter, i);
                        for (const auto & row_ref : mapped->rows)
                        {
                            added_columns.appendFromBlock<jf.add_missing>(row_ref.block_iter->block, row_ref.row_num);
                            ++current_offset;
                        }
                    }
                    else
                    {
                        /// In case when all rows were removed (by _tp_delta = -1)
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                    }
                }
                else if constexpr ((jf.is_latest_join) && jf.right)
                {
                    /// FIXME
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented path in joinColumns().");
                }
                else if constexpr (jf.is_latest_join && (jf.inner || jf.left))
                {
                    setUsed<need_filter>(filter, i);
                    added_columns.appendFromBlock<jf.add_missing>(mapped.block_iter->block, mapped.row_num);
                    break;
                }
                else /// ANY LEFT
                {
                    /// FIXME
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented path in joinColumns().");
                }
            }
        }

        if constexpr (has_null_map)
        {
            if (!right_row_found && null_element_found)
            {
                addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);

                if constexpr (jf.need_replication)
                {
                    (*added_columns.offsets_to_replicate)[i] = current_offset;
                }
                continue;
            }
        }

        /// Not add missing for 'right join left'
        if (!right_row_found && added_columns.is_left_block)
            addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);

        if constexpr (jf.need_replication)
            (*added_columns.offsets_to_replicate)[i] = current_offset;
    }

    added_columns.applyLazyDefaults();
    return filter;
}

template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map>
IColumn::Filter joinColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    return mapv.size() > 1 ? joinColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, true>(
               std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns)
                           : joinColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, false>(
                               std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
}

template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    bool has_null_map
        = std::any_of(added_columns.join_on_keys.begin(), added_columns.join_on_keys.end(), [](const auto & k) { return k.null_map; });
    if (added_columns.need_filter)
    {
        if (has_null_map)
            return joinColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, true>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        else
            return joinColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, false>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
    }
    else
    {
        if (has_null_map)
            return joinColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, true>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        else
            return joinColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, false>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
    }
}

template <Kind KIND, Strictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinColumns(const std::vector<const Maps *> & mapv, AddedColumns & added_columns, HashType type)
{
    constexpr bool is_asof_join = (STRICTNESS == Strictness::Asof || STRICTNESS == Strictness::Range);

    switch (type)
    {
#define M(TYPE) \
    case HashType::TYPE: { \
        using MapTypeVal = const typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<HashType::TYPE, MapTypeVal>::Type; \
        std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d) \
        { \
            const auto & join_on_key = added_columns.join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.push_back( \
                std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes))); \
        } \
        return joinColumnsSwitchNullability<KIND, STRICTNESS, KeyGetter>(std::move(key_getter_vector), a_map_type_vector, added_columns); \
    }
        APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
    }
}

/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <typename Map, typename KeyGetter>
struct Inserter
{
    static ALWAYS_INLINE void insertAll(
        const HashJoin &,
        Map & map,
        KeyGetter & key_getter,
        const JoinDataBlock * stored_block,
        size_t original_row,
        size_t row,
        Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, original_row, pool);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, row);
        else
            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
            emplace_result.getMapped().insert({stored_block, row}, pool);
    }

    static ALWAYS_INLINE void insertOne(
        const HashJoin & join, Map & map, KeyGetter & key_getter, JoinDataBlockList * blocks, size_t original_row, size_t row, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, original_row, pool);

        if (emplace_result.isInserted())
        {
            new (&emplace_result.getMapped()) typename Map::mapped_type(blocks, row);
        }
        else if (join.anyTakeLastRow())
        {
            /// We need explicitly destroy for RowRefWithRefCount case
            /// Then we can do proper garbage collection
            using T = typename Map::mapped_type;
            emplace_result.getMapped().~T();

            /// aggregates_pool->setCurrentTimestamps(window_lower_bound, window_upper_bound);

            new (&emplace_result.getMapped()) typename Map::mapped_type(blocks, row);
        }
    }

    static ALWAYS_INLINE void insertMultiple(
        const HashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        JoinDataBlockList * blocks,
        size_t original_row,
        size_t row,
        Arena & pool,
        IColumn::Filter * new_keys_filter)
    {
        auto emplace_result = key_getter.emplaceKey(map, original_row, pool);
        auto * mapped = &emplace_result.getMapped();

        if (emplace_result.isInserted())
        {
            mapped = new (mapped) typename Map::mapped_type(std::make_unique<HashJoin::RefListMultiple>());
            if (new_keys_filter)
                (*new_keys_filter)[original_row] = 1;
        }

        [[maybe_unused]] auto iter = (*mapped)->insert(blocks, row);
    }

    static ALWAYS_INLINE void insertRangeAsof(
        HashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        const JoinDataBlock * stored_block,
        size_t original_row,
        size_t row, /// If we merge small data blocks, row and original row will be different
        Arena & pool,
        const IColumn & asof_column)
    {
        auto emplace_result = key_getter.emplaceKey(map, original_row, pool);
        typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);
        time_series_map->insert(asof_type, asof_column, stored_block, original_row, row);
    }

    static ALWAYS_INLINE void insertAsof(
        HashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        JoinDataBlockList * blocks,
        size_t original_row,
        size_t row,
        Arena & pool,
        const IColumn & asof_column,
        UInt64 keep_versions)
    {
        auto emplace_result = key_getter.emplaceKey(map, original_row, pool);
        typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);

        time_series_map->insert(asof_type, asof_column, blocks, original_row, row, join.getAsofInequality(), keep_versions);
    }
};

template <Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
size_t NO_INLINE insertFromBlockImplTypeCase(
    HashJoin & join,
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    JoinDataBlockList * blocks,
    size_t start_row,
    ConstNullMapPtr null_map,
    Arena & pool,
    IColumn::Filter * new_keys_filter)
{
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, typename HashJoin::MapsOne::MappedType>;
    [[maybe_unused]] constexpr bool mapped_multiple
        = std::is_same_v<typename Map::mapped_type, typename HashJoin::MapsMultiple::MappedType>;

    constexpr bool is_range_asof_join = (STRICTNESS == Strictness::Range);
    constexpr bool is_asof_join = (STRICTNESS == Strictness::Asof);

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_range_asof_join || is_asof_join)
        asof_column = key_columns.back();

    auto key_getter = createKeyGetter < KeyGetter, is_range_asof_join || is_asof_join > (key_columns, key_sizes);

    for (size_t i = start_row; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            continue;

        if constexpr (is_range_asof_join)
            Inserter<Map, KeyGetter>::insertRangeAsof(
                join, map, key_getter, &blocks->lastDataBlock(), i - start_row, i, pool, *asof_column);
        else if constexpr (is_asof_join)
            Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, blocks, i - start_row, i, pool, *asof_column, join.keepVersions());
        else if constexpr (mapped_one)
            Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, blocks, i - start_row, i, pool);
        else if constexpr (mapped_multiple)
            /// So far \new_keys_filter is just used in multiple map for bidirectional join
            Inserter<Map, KeyGetter>::insertMultiple(join, map, key_getter, blocks, i - start_row, i, pool, new_keys_filter);
        else
            Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, &blocks->lastDataBlock(), i - start_row, i, pool);
    }
    return map.getBufferSizeInCells();
}

template <Strictness STRICTNESS, typename KeyGetter, typename Map>
size_t insertFromBlockImplType(
    HashJoin & join,
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    JoinDataBlockList * blocks,
    size_t start_row,
    ConstNullMapPtr null_map,
    Arena & pool,
    IColumn::Filter * new_keys_filter)
{
    if (null_map)
        return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(
            join, map, rows, key_columns, key_sizes, blocks, start_row, null_map, pool, new_keys_filter);
    else
        return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(
            join, map, rows, key_columns, key_sizes, blocks, start_row, null_map, pool, new_keys_filter);
}

template <Strictness STRICTNESS, typename Maps>
size_t insertFromBlockImpl(
    HashJoin & join,
    HashType type,
    Maps & maps,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    JoinDataBlockList * blocks,
    size_t start_row,
    ConstNullMapPtr null_map,
    Arena & pool,
    IColumn::Filter * new_keys_filter)
{
    switch (type)
    {
#define M(TYPE) \
    case HashType::TYPE: \
        return insertFromBlockImplType< \
            STRICTNESS, \
            typename KeyGetterForType<HashType::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            join, *maps.TYPE, rows, key_columns, key_sizes, blocks, start_row, null_map, pool, new_keys_filter); \
        break;
        APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
    }
}

[[maybe_unused]] inline void addDeltaColumn(Block & block, size_t rows)
{
    /// Add _tp_delta = 1 column to result block
    auto delta_type = DataTypeFactory::instance().get(TypeIndex::Int8);
    auto delta_col = delta_type->createColumn();
    delta_col->insertMany(1, rows);
    block.insert({std::move(delta_col), delta_type, ProtonConsts::RESERVED_DELTA_FLAG});
}
}

/// [INNER] JOIN : only matching rows are returned
/// LEFT [OUTER] JOIN: non-matching rows from left stream are returned in addition to matching rows
/// RIGHT [OUTER] JOIN: non-matching rows from right stream are returned in addition to matching rows
/// FULL [OUTER] JOIN: non-matching rows from both streams are returned in addition to matching rows
/// CROSS JOIN: produces a cartesian product of the 2 streams. "join keys" are not specified
///
/// Non-standard join types:
/// INNER LATEST JOIN : convert left / right stream to (versioned) kv and execute versioned kv join after
/// LEFT / RIGHT ANY JOIN: partially (for opposite side of LEFT and RIGHT) disables the cartesian product for standard JOIN types
/// INNER ANY JOIN: completely disables the cartesian product for standard JOIN types
/// ASOF JOIN: Closest match (non-equal) join
/// LEFT ASOF JOIN : non-matching rows from left stream are returned in addition to ASOF JOIN matching rows
/// LEFT / RIGHT SEMI JOIN: a whitelist on "join keys", without producing a cartesian product
/// LEFT / RIGHT ANTI JOIN: a blacklist on "join keys", without producing a cartesian product

/// Difference between `LEFT SEMI` and `LEFT ANY`
/// `LEFT SEMI` only produce matching rows on join keys and one left row only joins one right row
/// `LEFT ANY` produce non-matching rows from left stream + `left semi`

/// `ANTI JOIN` looks weird in streaming join but in some troubleshooting scenarios in historical join, it is handy since it is returning non-matching rows only
/// `left anti`: if there are non-matching rows from left stream, returning these non-matching rows with default value filling for right projected columns
/// `right anti`: if there are non-matching rows from right stream, returning these non-matching rows with default value filling left projected columns

/// A 4 dimensions array : Left StorageSemantic, Kind, Strictness, Right StorageSemantic
/// There are 4 * 6 * 5 * 4 = 480 total combinations
const HashJoin::SupportMatrix HashJoin::support_matrix = {
    /// <left_stroage_semantic, join_kind, join_strictness, right_storage_semantic> - supported
    /// Append ...
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},

    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Asof, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Asof, StorageSemantic::VersionedKV}, true},

    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Asof, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Asof, StorageSemantic::VersionedKV}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},

    /// Changelog ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},

    /// ChangelogKV ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},

    /// VersionedKV ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
};

void HashJoin::validate(const JoinCombinationType & join_combination)
{
    auto iter = support_matrix.find(join_combination);
    if (iter == support_matrix.end() || !(iter->second))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "'{} stream' {} {} JOIN '{} stream' is not supported",
            magic_enum::enum_name(std::get<0>(join_combination)),
            toString(std::get<1>(join_combination)),
            toString(std::get<2>(join_combination)),
            magic_enum::enum_name(std::get<3>(join_combination)));
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    JoinStreamDescriptionPtr left_join_stream_desc_,
    JoinStreamDescriptionPtr right_join_stream_desc_)
    : table_join(std::move(table_join_))
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(false)
    , asof_inequality(table_join->getAsofInequality())
    , right_data(std::move(right_join_stream_desc_))
    , left_data(std::move(left_join_stream_desc_))
    , logger(&Poco::Logger::get("StreamingHashJoin"))
{
    init();

    initRightBlockStructure();

    chooseHashMethod();

    initHashMaps(right_data.buffered_data->getCurrentMapsVariants().map_variants);

    LOG_INFO(
        logger,
        "({}) hash type: {}, kind: {}, strictness: {}, keys : {}, buffered_data_block_size={} right header: {}",
        fmt::ptr(this),
        toString(hash_method_type),
        toString(kind),
        toString(strictness),
        TableJoin::formatClauses(table_join->getClauses(), true),
        dataBlockSize(),
        right_data.join_stream_desc->input_header.dumpStructure());

    if (streaming_strictness == Strictness::Range)
        LOG_INFO(
            logger,
            "Range join: {} join_start_bucket={} join_stop_bucket={}",
            table_join->rangeAsofJoinContext().string(),
            left_data.buffered_data->join_start_bucket_offset,
            left_data.buffered_data->join_stop_bucket_offset);
}

HashJoin::~HashJoin() noexcept
{
    LOG_INFO(logger, "{}", metricsString());
}

void HashJoin::init()
{
    checkJoinSemantic();

    streaming_kind = Streaming::toJoinKind(kind);
    streaming_strictness = Streaming::toJoinStrictness(strictness, table_join->isRangeJoin());

    emit_changelog
        = isJoinResultChangelog(left_data.join_stream_desc->data_stream_semantic, right_data.join_stream_desc->data_stream_semantic);

    retract_push_down
        = (isVersionedKeyedStorage(left_data.join_stream_desc->data_stream_semantic)
           && isVersionedKeyedStorage(right_data.join_stream_desc->data_stream_semantic) && right_data.join_stream_desc->hasPrimaryKey());

    retract_push_down
        |= (isChangelogKeyedStorage(left_data.join_stream_desc->data_stream_semantic)
            && isChangelogKeyedStorage(right_data.join_stream_desc->data_stream_semantic) && right_data.join_stream_desc->hasPrimaryKey());

    /// So far there are following cases which doesn't require bidirectional join
    /// SELECT * FROM append_only INNER ALL JOIN versioned_kv ON append_only.key = versioned_kv.key;
    /// SELECT * FROM append_only INNER LATEST JOIN versioned_kv ON append_only.key = versioned_kv.key;
    /// SELECT * FROM append_only INNER ASOF JOIN versioned_kv ON append_only.key = versioned_kv.key AND append_only.timestamp < versioned_kv.timestamp SETTINGS keep_versions=3;
    /// SELECT * FROM left_append_only INNER ASOF JOIN right_append_only ON left_append_only.key = right_append_only.key AND left_append_only.timestamp < right_append_only.timestamp SETTINGS keep_versions=3;
    /// SELECT * FROM left_append_only INNER LATEST JOIN right_append_only ON left_append_only.key = right_append_only.key;
    /// `ASOF` keeps multiple versions and `LATEST` only keeps the latest version for the join key
    auto data_enrichment_join = (left_data.join_stream_desc->data_stream_semantic == DataStreamSemantic::Append
                                 && right_data.join_stream_desc->data_stream_semantic == DataStreamSemantic::Changelog)
        || streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Latest;

    bidirectional_hash_join = !data_enrichment_join;

    /// append-only inner join append-only on ... and date_diff_within(10s)
    /// In case when emitChangeLog()
    if (streaming_strictness == Strictness::Range
        && (left_data.join_stream_desc->data_stream_semantic != DataStreamSemantic::Append
            || right_data.join_stream_desc->data_stream_semantic != DataStreamSemantic::Append
            || streaming_kind != Kind::Inner))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only support inner range join append-only streams");

    range_bidirectional_hash_join = bidirectional_hash_join && (streaming_strictness == Strictness::Range);

    reviseJoinStrictness();

    /// If right stream is key-value data stream semantic and our query plan pushes down the changelog transform
    /// initRightPrimaryKeyHashTable();

    initBufferedData();

    /// Cache condition column names
    const auto & on_exprs = table_join->getClauses();
    cond_column_names.reserve(on_exprs.size());
    for (const auto & on_expr : on_exprs)
        cond_column_names.push_back(on_expr.condColumnNames());

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(
            key_names_right, right_data.join_stream_desc->input_header, right_data.table_keys, right_data.sample_block_with_columns_to_add);
        right_data.required_keys = table_join->getRequiredRightKeys(right_data.table_keys, right_data.required_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        /// SELECT * FROM left JOIN right ON left.key = right.key OR left.value = right.value
        right_data.sample_block_with_columns_to_add = right_data.table_keys = materializeBlock(right_data.join_stream_desc->input_header);
    }
}

void HashJoin::initLeftPrimaryKeyHashTable()
{
    /// If left stream is key-value data stream semantic and our query plan pushes down the changelog transform
    /// versioned_kv join versioned_kv and chaneglog_kv join changelog_kv
}

void HashJoin::initRightPrimaryKeyHashTable()
{
    if (!right_data.join_stream_desc->hasPrimaryKey())
        return;

    if (streaming_strictness == Streaming::Strictness::Asof || streaming_strictness == Streaming::Strictness::Range
        || streaming_strictness == Streaming::Strictness::Latest)
        return;

    /// versioned_kv join versioned_kv and chaneglog_kv join changelog_kv

    /// When `append-only` stream join `versioned-kv`, if we didn't join on primary key columns fully
    /// We will need maintain extra data structures to figure out if a row is one with a new primary key
    /// or an update to an existing primary key shown before
    const auto & clause = table_join->getClauses().front();
    const auto & key_names_right = clause.key_names_right;

    ColumnRawPtrs primary_key_columns;
    primary_key_columns.reserve(right_data.join_stream_desc->primary_key_column_positions->size());

    bool join_on_partial_primary_key_columns = false;
    for (const auto & key_col_pos : right_data.join_stream_desc->primary_key_column_positions.value())
    {
        const auto & key_col = right_data.join_stream_desc->input_header.getByPosition(key_col_pos);
        primary_key_columns.push_back(key_col.column.get());

        if (std::find(key_names_right.begin(), key_names_right.end(), key_col.name) == key_names_right.end())
            join_on_partial_primary_key_columns = true;
    }

    /// If all primary key columns are joined, then do nothing
    if (!join_on_partial_primary_key_columns)
        return;

    auto [primary_key_hash_method_type, primary_key_size] = Streaming::chooseHashMethod(primary_key_columns);

    right_data.primary_key_hash_table = std::make_shared<PrimaryKeyHashTable>(primary_key_hash_method_type, std::move(primary_key_size));

    LOG_INFO(logger, "Partial primary key join push down case");
}

/// Left stream header is only known at late stage after HashJoin is created
void HashJoin::postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_)
{
    if (output_header)
        return;

    join_max_cached_bytes = join_max_cached_bytes_;
    left_data.join_stream_desc->input_header = left_header;
    output_header = output_header_;

    /// Now, we can evaluate the primary key column positions for left join stream desc since left header is ready
    left_data.join_stream_desc->calculateColumnPositions(strictness);

    /// initLeftPrimaryKeyHashTable();

    validateAsofJoinKey();

    /// If it is not bidirectional hash join, we don't care left header
    if (bidirectional_hash_join)
    {
        /// left stream sample block's column may be not inited yet
        JoinCommon::createMissedColumns(left_data.join_stream_desc->input_header);
        if (table_join->oneDisjunct())
        {
            const auto & key_names_left = table_join->getOnlyClause().key_names_left;
            JoinCommon::splitAdditionalColumns(
                key_names_left, left_data.join_stream_desc->input_header, left_data.table_keys, left_data.sample_block_with_columns_to_add);
            left_data.required_keys = table_join->getRequiredLeftKeys(left_data.table_keys, left_data.required_keys_sources);
        }
        else
        {
            left_data.sample_block_with_columns_to_add = left_data.table_keys = materializeBlock(left_data.join_stream_desc->input_header);
        }

        initLeftBlockStructure();

        initHashMaps(left_data.buffered_data->getCurrentMapsVariants().map_variants);

        if (retract_push_down && emit_changelog)
        {
            join_results.emplace(output_header);
            initHashMaps(join_results->maps->map_variants);
        }

        if (emitChangeLog())
        {
            /// We'd like to compute some reserved column positions for `the right block join left block` case
            /// since we will need swap them before project the join results
            if (left_data.join_stream_desc->hasDeltaColumn() && right_data.join_stream_desc->hasDeltaColumn())
            {
                Block right_join_left_header = right_data.join_stream_desc->input_header;
                doJoinBlockWithHashTable<false>(right_join_left_header, left_data.buffered_data->getCurrentHashBlocksPtr());

                for (size_t i = 0; const auto & col : right_join_left_header)
                {
                    if (col.name == ProtonConsts::RESERVED_DELTA_FLAG)
                        left_delta_column_position_rlj = i;
                    else if (col.name.ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
                        right_delta_column_position_rlj = i;

                    ++i;
                }

                /// We'd like to compute some reserved column positions for `the left block join right block` case
                /// since we will need swap them before project the join results
                Block left_join_right_header = left_data.join_stream_desc->input_header;
                doJoinBlockWithHashTable<true>(left_join_right_header, right_data.buffered_data->getCurrentHashBlocksPtr());
                for (size_t i = 0; const auto & col : left_join_right_header)
                {
                    if (col.name == ProtonConsts::RESERVED_DELTA_FLAG)
                        left_delta_column_position_lrj = i;
                    else if (col.name.ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
                        right_delta_column_position_lrj = i;

                    ++i;
                }
            }
        }
    }
}

void HashJoin::transformHeader(Block & header)
{
    if (range_bidirectional_hash_join)
        joinBlockWithHashTable<true>(header, right_data.buffered_data->getCurrentHashBlocksPtr());
    else if (bidirectional_hash_join)
        joinBlockWithHashTable<true>(header, right_data.buffered_data->getCurrentHashBlocksPtr());
    else
        joinLeftBlock(header);

    /// Remove internal left/right delta column
    {
        std::set<size_t> delta_pos;
        for (size_t pos = 0; auto & col_with_type_name : header)
        {
            if (col_with_type_name.name.ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
                delta_pos.emplace(pos);

            ++pos;
        }
        header.erase(delta_pos);
    }
    assert(!header.has(ProtonConsts::RESERVED_DELTA_FLAG));

    /// Apppend joined delta column
    if (emitChangeLog())
        header.insert({DataTypeFactory::instance().get(TypeIndex::Int8), ProtonConsts::RESERVED_DELTA_FLAG});
}

void HashJoin::chooseHashMethod()
{
    size_t disjuncts_num = table_join->getClauses().size();
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_data.table_keys, key_names_right);

        if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        {
            assert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ASOF join needs at least one equal-join column");

            if (right_data.table_keys.getByName(key_names_right.back()).type->isNullable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join over right stream Nullable column is not implemented");

            size_t asof_size;
            asof_type = getAsofTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            std::tie(hash_method_type, asof_key_sizes) = Streaming::chooseHashMethod(key_columns);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto & new_key_sizes = key_sizes.emplace_back();
            std::tie(hash_method_type, new_key_sizes) = Streaming::chooseHashMethod(key_columns);
        }
    }
}

void HashJoin::dataMapInit(MapsVariant & map)
{
    [[maybe_unused]] auto inited = joinDispatchInit(streaming_kind, streaming_strictness, map);
    assert(inited);

    inited = joinDispatch(streaming_kind, streaming_strictness, map, [this](auto, auto, auto & map_) { map_.create(hash_method_type); });
    assert(inited);
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    return false;
}

size_t HashJoin::getTotalRowCount() const
{
    size_t res = 0;
    /// FIXME
    //    for (const auto & map : right_data.buffered_data->maps)
    //    {
    //        joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalRowCount(right_data.buffered_data->type); });
    //    }

    return res;
}

size_t HashJoin::getTotalByteCount() const
{
    size_t res = 0;
    /// FIXME

    //    for (const auto & map : right_data.buffered_data->maps)
    //    {
    //        joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(right_data.buffered_data->type); });
    //    }
    //    res += right_data.buffered_data->pool.size();

    return res;
}

void HashJoin::initLeftBlockStructure()
{
    assert(bidirectionalHashJoin());

    JoinCommon::convertToFullColumnsInplace(left_data.table_keys);

    initBlockStructure(left_data, left_data.table_keys, left_data.sample_block_with_columns_to_add);

    /// Remove un-needed left `key` columns
    /// SELECT left.key FROM left INNER / LEFT / RIGHT JOIN right ON left.key = right.key
    /// right_data.required_keys : []
    /// left_data.required_keys : [left.key]
    /// When we use right block to join left blocks, right block already has `right.key`,
    /// we don't need add `left.key` to the joined block since we can simply "rename" `right.key` to `left.key`.
    /// Actually there are 3 cases
    /// 1) INNER JOIN, renaming is just ok since the keys are equal when they are joined
    /// 2) LEFT JOIN, renaming is just ok as well. Joined means equal, not joined empty result
    /// 3) RIGHT JOIN, renaming will not be OK for non-joined rows (FIXME)
    JoinCommon::createMissedColumns(left_data.sample_block_with_columns_to_add);

    /// right_data.required_keys + right_data.sample_block_with_column_to_add is the projection header
    /// left_data.required_keys + left_data.sample_block_with_column_to_add is the projection header

    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// It should be nullable if nullable_right_side is true
        left_data.asof_key_column = savedLeftBlockSample().getByName(table_join->getOnlyClause().key_names_left.back());
}

void HashJoin::initRightBlockStructure()
{
    JoinCommon::convertToFullColumnsInplace(right_data.table_keys);

    initBlockStructure(right_data, right_data.table_keys, right_data.sample_block_with_columns_to_add);

    JoinCommon::createMissedColumns(right_data.sample_block_with_columns_to_add);

    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// It should be nullable if nullable_right_side is true
        right_data.asof_key_column = savedRightBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::initBlockStructure(JoinData & join_data, const Block & table_keys, const Block & sample_block_with_columns_to_add) const
{
    Block & saved_block_sample = join_data.buffered_data->sample_block;

    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) || isRightOrFull(kind) || multiple_disjuncts;
    if (save_key_columns)
        saved_block_sample = table_keys.cloneEmpty();
    else if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// Save ASOF key
        saved_block_sample.insert(table_keys.safeGetByPosition(table_keys.columns() - 1));

    /// Save non key columns
    for (const auto & column : sample_block_with_columns_to_add)
    {
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }

    /// Cache delta position in saved block for future use
    std::vector<size_t> reserved_column_positions;
    if (join_data.join_stream_desc->hasDeltaColumn())
    {
        const auto & delta_column_name = join_data.join_stream_desc->deltaColumnName();

        if (saved_block_sample.has(delta_column_name))
            reserved_column_positions.push_back(saved_block_sample.getPositionByName(delta_column_name));
    }

    for (size_t pos = 0; const auto & col : saved_block_sample)
    {
        if (col.name == ProtonConsts::RESERVED_EVENT_TIME || col.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID
            || col.name == ProtonConsts::RESERVED_APPEND_TIME || col.name == ProtonConsts::RESERVED_INDEX_TIME)
            reserved_column_positions.push_back(pos);
        else if (
            col.name.ends_with(ProtonConsts::RESERVED_EVENT_TIME) || col.name.ends_with(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
            || col.name.ends_with(ProtonConsts::RESERVED_APPEND_TIME) || col.name.ends_with(ProtonConsts::RESERVED_INDEX_TIME))
            reserved_column_positions.push_back(pos);

        ++pos;
    }

    if (!reserved_column_positions.empty())
        join_data.buffered_data->reserved_column_positions.emplace(std::move(reserved_column_positions));
}

Block HashJoin::prepareBlockToSave(const Block & block, const Block & sample_block)
{
    Block structured_block;

    structured_block.info = block.info;

    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);

        if (column.column->lowCardinality() && !sample_column.column->lowCardinality())
        {
            column.column = column.column->convertToFullColumnIfLowCardinality();
            column.type = removeLowCardinality(column.type);
        }

        /// There's no optimization for right side const columns. Remove const-ness if any.
        column.column = recursiveRemoveSparse(column.column->convertToFullColumnIfConst());
        structured_block.insert(std::move(column));
    }

    return structured_block;
}

template <bool is_left_block>
Block HashJoin::prepareBlock(const Block & block) const
{
    if constexpr (is_left_block)
        return prepareBlockToSave(block, savedLeftBlockSample());
    else
        return prepareBlockToSave(block, savedRightBlockSample());
}

bool HashJoin::addJoinedBlock(const Block & block, bool /*check_limits*/)
{
    doInsertBlock<false>(block, right_data.buffered_data->getCurrentHashBlocksPtr()); /// Copy the block
    return true;
}

template <bool is_left_block>
void HashJoin::doInsertBlock(Block block, HashBlocksPtr target_hash_blocks, IColumn::Filter * new_keys_filter)
{
    /// FIXME, there are quite some block copies
    /// FIXME, all_key_columns shall hold shared_ptr to columns instead of raw ptr
    /// then we can update `source_block` in place
    JoinTableSide side = JoinTableSide::Left;
    if constexpr (!is_left_block)
        side = JoinTableSide::Right;

    /// key columns are from source `block`
    ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(block, table_join->getAllNames(side));

    /// We have copy of source `block` to `block_to_save` after prepare, so `block_to_save` is good to get moved to the buffered stream data
    Block block_to_save = prepareBlock<is_left_block>(block);

    /// FIXME, multiple disjuncts OR clause
    const auto & on_expr = table_join->getClauses().front();

    ColumnRawPtrs key_columns;
    const Names * key_names;
    if constexpr (is_left_block)
        key_names = &on_expr.key_names_left;
    else
        key_names = &on_expr.key_names_right;

    key_columns.reserve(key_names->size());
    for (const auto & name : *key_names)
        key_columns.push_back(all_key_columns[name]);

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    /// If LEFT, RIGHT or FULL save blocks with nulls for NotJoinedBlocks
    bool is_not_inner;
    if constexpr (is_left_block)
        is_not_inner = isLeftOrFull(kind);
    else
        is_not_inner = isRightOrFull(kind);

    UInt8 save_nullmap = 0;
    if (is_not_inner && null_map)
    {
        /// Save rows with NULL keys
        for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
            save_nullmap |= (*null_map)[i];
    }

    /// Add `block_to_save` to target stream data
    /// Note `block_to_save` may be empty for cases in which the query doesn't care other non-key columns.
    /// For example, SELECT count() FROM stream_a JOIN stream_b ON i=ii;

    JoinData * join_data;
    if constexpr (is_left_block)
        join_data = &left_data;
    else
        join_data = &right_data;

    auto start_row = join_data->buffered_data->addOrConcatDataBlockWithoutLock(std::move(block_to_save), target_hash_blocks);
    auto rows = target_hash_blocks->blocks.lastDataBlock().rows();

    joinDispatch(
        streaming_kind,
        streaming_strictness,
        target_hash_blocks->maps->map_variants[0],
        [&, this](auto /*kind_*/, auto strictness_, auto & map) {
            [[maybe_unused]] size_t size = insertFromBlockImpl<strictness_>(
                *this,
                hash_method_type,
                map,
                rows,
                key_columns,
                key_sizes[0],
                &target_hash_blocks->blocks,
                start_row,
                null_map,
                target_hash_blocks->pool,
                new_keys_filter);
        });

    if (save_nullmap)
        /// FIXME, we will need account the allocated bytes for null_map_holder / not_joined_map as well
        target_hash_blocks->blocks_nullmaps.emplace_back(&target_hash_blocks->lastDataBlock(), null_map_holder);

    checkLimits();
}

/// Join left block with right hash table or join right block with left hash table
template <bool is_left_block, Kind KIND, Strictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const
{
    /// joining -> the "left" side
    /// joined -> the "right" side
    /// When `is_left_block = false`, `joining` is actually the `right` stream, and `joined` is actually the `left` stream
    const JoinData * joining_data = &left_data;
    const JoinData * joined_data = &right_data;

    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    const auto & on_exprs = table_join->getClauses();
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(on_exprs.size());
    for (size_t i = 0; i < on_exprs.size(); ++i)
    {
        if constexpr (is_left_block)
            join_on_keys.emplace_back(block, on_exprs[i].key_names_left, cond_column_names[i].first, key_sizes[i]);
        else
            join_on_keys.emplace_back(block, on_exprs[i].key_names_right, cond_column_names[i].second, key_sizes[i]);
    }

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" stream must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    /// if constexpr (jf.right || jf.full)
    /// {
    ///    materializeBlockInplace(left_block);
    /// }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */

    const Block * joined_sample_block;
    if constexpr (is_left_block)
        joined_sample_block = &savedRightBlockSample();
    else
        joined_sample_block = &savedLeftBlockSample();

    AddedColumns added_columns(
        block_with_columns_to_add,
        block,
        *joined_sample_block,
        *this,
        std::move(join_on_keys),
        joining_data->buffered_data->range_asof_join_ctx,
        jf.is_asof_join || jf.is_range_join,
        is_left_block);

    bool has_required_joined_keys = (joined_data->required_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_joined_keys;

    IColumn::Filter row_filter = switchJoinColumns<KIND, STRICTNESS>(maps_, added_columns, hash_method_type);

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> joined_keys_to_replicate [[maybe_unused]];

    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        /// FIXME, what it actually does ?
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from joined block if needed using value from joining table because of equality
        for (size_t i = 0; i < joined_data->required_keys.columns(); ++i)
        {
            const auto & joined_key = joined_data->required_keys.getByPosition(i);
            // renamed ???
            if (!block.findByName(joined_key.name))
            {
                /// asof or range column is already in block.
                const String * joined_key_name_in_clause;
                if constexpr (is_left_block)
                    joined_key_name_in_clause = &table_join->getOnlyClause().key_names_right.back();
                else
                    joined_key_name_in_clause = &table_join->getOnlyClause().key_names_left.back();

                if ((jf.is_asof_join || jf.is_range_join) && joined_key.name == *joined_key_name_in_clause)
                    continue;

                const auto & joining_col_name = joined_data->required_keys_sources[i];
                const auto & col = block.getByName(joining_col_name);
                bool is_nullable = JoinCommon::isNullable(joined_key.type);

                /// We need rename right column name if `is_left_block is true`
                /// Otherwise keep the column name as it is as column from actual left block don't require a renaming
                ColumnWithTypeAndName joined_key_col(
                    col.column, col.type, is_left_block ? getTableJoin().renamedRightColumnName(joined_key.name) : joined_key.name);
                if (joined_key_col.type->lowCardinality() != joined_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(joined_key_col);
                joined_key_col = JoinCommon::correctNullability(std::move(joined_key_col), is_nullable);
                block.insert(std::move(joined_key_col));
            }
        }
    }
    else if (has_required_joined_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add joined key columns from joined block if needed.
        for (size_t i = 0; i < joined_data->required_keys.columns(); ++i)
        {
            const auto & joined_key = joined_data->required_keys.getByPosition(i);
            auto joined_col_name = is_left_block ? getTableJoin().renamedRightColumnName(joined_key.name) : joined_key.name;
            if (!block.findByName(joined_col_name))
            {
                /// asof column is already in block.
                const String * joined_key_name_in_clause;
                if constexpr (is_left_block)
                    joined_key_name_in_clause = &table_join->getOnlyClause().key_names_right.back();
                else
                    joined_key_name_in_clause = &table_join->getOnlyClause().key_names_left.back();

                if ((jf.is_asof_join || jf.is_range_join) && joined_key.name == *joined_key_name_in_clause)
                    continue;

                const auto & joining_col_name = joined_data->required_keys_sources[i];
                const auto & col = block.getByName(joining_col_name);
                bool is_nullable = JoinCommon::isNullable(joined_key.type);

                ColumnPtr thin_column = JoinCommon::filterWithBlanks(col.column, filter);

                ColumnWithTypeAndName joined_key_col(thin_column, col.type, joined_col_name);
                if (joined_key_col.type->lowCardinality() != joined_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(joined_key_col);
                joined_key_col = JoinCommon::correctNullability(std::move(joined_key_col), is_nullable, null_map_filter);
                block.insert(std::move(joined_key_col));

                if constexpr (jf.need_replication)
                    joined_keys_to_replicate.push_back(block.getPositionByName(joined_key.name));
            }
        }
    }

    if constexpr (jf.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional joined keys
        for (size_t pos : joined_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_data.table_keys, onexpr.key_names_right);
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & /*not_processed*/)
{
    joinLeftBlock(block);
}

template <bool is_left_block>
void HashJoin::doJoinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks)
{
    JoinData * joining_data = &left_data;
    JoinData * joined_data = &right_data;
    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    if (unlikely(!joining_data->validated_join_key_types))
    {
        const auto & join_clauses = table_join->getClauses();
        for (size_t i = 0; i < join_clauses.size(); ++i)
        {
            const auto & onexpr = join_clauses[i];
            const auto & cond_column_name = cond_column_names[i];

            JoinCommon::checkTypesOfKeys(
                block,
                is_left_block ? onexpr.key_names_left : onexpr.key_names_right,
                is_left_block ? cond_column_name.first : cond_column_name.second,
                joined_data->join_stream_desc->input_header,
                is_left_block ? onexpr.key_names_right : onexpr.key_names_left,
                is_left_block ? cond_column_name.second : cond_column_name.first);
        }

        joining_data->validated_join_key_types = true;
    }

    std::vector<const std::decay_t<decltype(target_hash_blocks->maps->map_variants[0])> *> maps_vector;
    for (size_t i = 0; i < table_join->getClauses().size(); ++i)
        maps_vector.push_back(&target_hash_blocks->maps->map_variants[i]);

    auto flipped_kind = streaming_kind;
    if constexpr (!is_left_block)
        flipped_kind = flipKind(streaming_kind);

    if (joinDispatch(flipped_kind, streaming_strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_) {
            joinBlockImpl<is_left_block, kind_, strictness_>(block, joined_data->sample_block_with_columns_to_add, maps_vector_);
        }))
    {
        /// Joined
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
}

/// Use left_block to join right hash table
void HashJoin::joinLeftBlock(Block & left_block)
{
    assert(!bidirectional_hash_join && !range_bidirectional_hash_join && !emit_changelog);

    /// SELECT * FROM append_only [INNER | LEFT | RIGHT | FULL] JOIN versioned_kv
    /// SELECT * FROM append_only ASOF JOIN versioned_kv
    /// SELECT * FROM append_only ASOF JOIN right_append_only

    std::scoped_lock lock(right_data.buffered_data->mutex);

    doJoinBlockWithHashTable<true>(left_block, right_data.buffered_data->getCurrentHashBlocksPtr());
    transformToOutputBlock<true>(left_block);
}

template <bool is_left_block>
Block HashJoin::joinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks)
{
    assert(bidirectional_hash_join || range_bidirectional_hash_join);

    doJoinBlockWithHashTable<is_left_block>(block, std::move(target_hash_blocks));

    if (retract_push_down && emit_changelog)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Retraction push down is not implemented");

        // if (!block.has(ProtonConsts::RESERVED_DELTA_FLAG))
        //     addDeltaColumn(block);

        // transformToOutputBlock<is_left_block>(block);

        // return retract(block);
    }

    transformToOutputBlock<is_left_block>(block);
    return {};
}

template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, typename HashJoin::MapsAsof::MappedType>;
        constexpr bool mapped_range_asof = std::is_same_v<Mapped, typename HashJoin::MapsRangeAsof::MappedType>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, typename HashJoin::MapsOne::MappedType>;

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_range_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = mapped.block->getByPosition(j).column;
                columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
            }

            ++rows_added;
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                for (size_t j = 0; j < columns_right.size(); ++j)
                {
                    const auto & mapped_column = it->block->getByPosition(j).column;
                    columns_right[j]->insertFrom(*mapped_column, it->row_num);
                }

                ++rows_added;
            }
        }
    }
};

String HashJoin::metricsString() const
{
    return fmt::format(
        "Left stream metrics: {{{}}}, right stream metrics: {{{}}}, global join metrics: {{{}}}, "
        "retract buffer metrics: {{{}}}",
        left_data.buffered_data->joinMetricsString(),
        right_data.buffered_data->joinMetricsString(),
        join_metrics.string(),
        join_results ? join_results->joinMetricsString(this) : "");
}

std::shared_ptr<NotJoinedBlocks>
HashJoin::getNonJoinedBlocks(const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    /// FIXME, if there are left rows which are not joined with right data
    return {};
}

void HashJoin::initBufferedData()
{
    if (streaming_strictness == Strictness::Range)
    {
        const auto & right_asof_key_col = table_join->getOnlyClause().key_names_right.back();
        const auto & left_asof_key_col = table_join->getOnlyClause().key_names_left.back();

        const auto & range_join_ctx = table_join->rangeAsofJoinContext();

        right_data.buffered_data = std::make_unique<BufferedStreamData>(this, range_join_ctx, right_asof_key_col);
        left_data.buffered_data = std::make_unique<BufferedStreamData>(this, range_join_ctx, left_asof_key_col);
    }
    else
    {
        /// Although `append-only` joining `versioned-kv` doesn't need hold left data in memory
        /// we init the left_data to handle different case uniformly
        right_data.buffered_data = std::make_unique<BufferedStreamData>(this);
        left_data.buffered_data = std::make_unique<BufferedStreamData>(this);
    }
}

void HashJoin::initHashMaps(std::vector<MapsVariant> & all_maps)
{
    all_maps.resize(table_join->getClauses().size());
    for (auto & maps : all_maps)
        dataMapInit(maps);
}

void HashJoin::insertRightBlock(Block right_block)
{
    assert(!range_bidirectional_hash_join && !bidirectional_hash_join);

    std::scoped_lock lock(right_data.buffered_data->mutex);

    if (isRetractBlock(right_block, *right_data.join_stream_desc))
    {
        eraseExistingKeys<false>(right_block, right_data);
        return;
    }

    /// TODO... changelog transform push down optimization is not implemented yet
    /// auto row_refs = eraseOrAppendForPartialPrimaryKeyJoin(right_block);
    doInsertBlock<false>(std::move(right_block), right_data.buffered_data->getCurrentHashBlocksPtr());
}

Block HashJoin::insertLeftBlockAndJoin(Block & left_block)
{
    assert(bidirectional_hash_join && !range_bidirectional_hash_join);

    /// For bidirectional join, process one stream at a time
    std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

    if (emitChangeLog())
    {
        /// If this is a retract block which contains _tp_delta with -1 as it value
        if (auto joined_retracted_block = eraseExistingKeysAndRetractJoin<true>(left_block); joined_retracted_block)
            return std::move(*joined_retracted_block);
    }
    else
    {
        /// Just erase the existing keys from hash map on left side, no need retract join
        if (isRetractBlock(left_block, *left_data.join_stream_desc))
        {
            eraseExistingKeys<true>(left_block, left_data);
            left_block.clear();
            return {};
        }
    }

    doInsertBlock<true>(left_block, left_data.buffered_data->getCurrentHashBlocksPtr());

    return joinBlockWithHashTable<true>(left_block, right_data.buffered_data->getCurrentHashBlocksPtr());
}

Block HashJoin::insertRightBlockAndJoin(Block & right_block)
{
    assert(output_header);
    assert(bidirectional_hash_join && !range_bidirectional_hash_join);

    std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

    if (emitChangeLog())
    {
        /// If this is a retract block which contains _tp_delta with -1 as it value
        if (auto joined_retracted_block = eraseExistingKeysAndRetractJoin<false>(right_block); joined_retracted_block)
            return std::move(*joined_retracted_block);
    }
    else
    {
        /// Just erase the existing keys from hash map on right side, no need retract join
        if (isRetractBlock(right_block, *right_data.join_stream_desc))
        {
            eraseExistingKeys<false>(right_block, right_data);
            right_block.clear();
            return {};
        }
    }

    /// For left join, if right data have new join keys, we need delete prev joined result (join key + left_cols + right_cols<empty>),
    /// for example: `lkv left join rkv on lkv.key = rkv.key`
    ///         (lkv)           (rkv)               (output)
    /// (s1)    k1,v1,+1                    >>      k1, v1, (rkv.defaults), +1
    /// (s2)                    k1,v2,+1    >>      k1, v1, (rkv.defaults), -1
    ///                                             k1, v1, rkv.k1, rkv.v2, +1
    /// So, at (s2): we need to do right join left with `k1,(other defaults),-1` and `k1,v2,+1` 
    std::unique_ptr<IColumn::Filter> new_keys_filter;
    if (streaming_kind == Kind::Left && emitChangeLog())
        new_keys_filter = std::make_unique<IColumn::Filter>(right_block.rows(), 0);

    doInsertBlock<false>(right_block, right_data.buffered_data->getCurrentHashBlocksPtr(), new_keys_filter.get());
    if (new_keys_filter)
    {
        assert(right_data.join_stream_desc->hasDeltaColumn());
        /// 1) Generate an block with new keys and default values (such as `k1, (other defaults), -1`)
        MutableColumns columns;
        columns.reserve(right_block.columns());
        /// FIXME, multiple disjuncts OR clause
        const auto & key_names = table_join->getClauses().front().key_names_right;
        size_t new_keys_rows = std::accumulate(new_keys_filter->begin(), new_keys_filter->end(), static_cast<size_t>(0));
        for (auto & col_with_type_name : right_block)
        {
            if (std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; }))
                columns.emplace_back(IColumn::mutate(col_with_type_name.column->filter(*new_keys_filter, new_keys_rows)));
            else if (col_with_type_name.name == right_data.join_stream_desc->deltaColumnName())
            {
                columns.emplace_back(col_with_type_name.column->cloneEmpty());
                columns.back()->reserve(new_keys_rows);
                columns.back()->insertMany(-1, new_keys_rows);
            }
            else
            {
                columns.emplace_back(col_with_type_name.column->cloneEmpty());
                columns.back()->reserve(new_keys_rows);
                columns.back()->insertManyDefaults(new_keys_rows);
            }
        }

        /// 2) Do retract right <left join> left (such as `k1, v1, (rkv.defaults), -1`)
        Block retracted_block = right_block.cloneWithColumns(std::move(columns));
        auto pushdown_retracted = joinBlockWithHashTable<false>(retracted_block, left_data.buffered_data->getCurrentHashBlocksPtr());
        assert(!pushdown_retracted);
        /// NOTE: Reset the right join key columns to default values after joined, for example:
        /// `k1, v1, rkv.k1, (rkv.other_defaults), -1`  =>  `k1, v1, (rkv.defaults), -1`
        if (retracted_block.rows())
        {
            for (auto & col_with_type_name : retracted_block)
            {
                if (std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; }))
                {
                    auto key_col = IColumn::mutate(col_with_type_name.column->cloneEmpty());
                    key_col->reserve(right_block.rows());
                    key_col->insertManyDefaults(right_block.rows());
                    col_with_type_name.column = std::move(key_col);
                }
            }
        }

        /// 3) Do right <left join> left (such as `k1, v1, v2, +1`)
        pushdown_retracted = joinBlockWithHashTable<false>(right_block, left_data.buffered_data->getCurrentHashBlocksPtr());
        assert(!pushdown_retracted);

        return retracted_block;
    }

    return joinBlockWithHashTable<false>(right_block, left_data.buffered_data->getCurrentHashBlocksPtr());
}

template <bool is_left_block>
std::vector<Block> HashJoin::insertBlockToRangeBucketsAndJoin(Block block)
{
    assert(block.rows());
    assert(range_bidirectional_hash_join);

    auto * joining_data = left_data.buffered_data.get();
    auto * joined_data = right_data.buffered_data.get();
    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    /// Here we explicitly order the lock of the mutex to avoid deadlock
    std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

    /// Insert
    auto bucket_blocks = joining_data->assignDataBlockToRangeBuckets(std::move(block));
    for (auto & bucket_block : bucket_blocks)
        /// Here we copy over the block since the block will be used to join which will modify the columns in-place
        doInsertBlock<is_left_block>(bucket_block.block, std::move(bucket_block.hash_blocks));

    std::vector<Block> joined_blocks;
    joined_blocks.reserve(bucket_blocks.size());

    /// Join
    for (auto & bucket_block : bucket_blocks)
    {
        auto joining_bucket = static_cast<Int64>(bucket_block.bucket);

        /// Find the range buckets to join
        auto & joined_range_bucket_hash_blocks = joined_data->getRangeBucketHashBlocks();

        auto bucket_offset = joining_data->join_start_bucket_offset;
        if constexpr (!is_left_block)
            bucket_offset = joining_data->join_stop_bucket_offset;

        auto lower_bound = joining_bucket - bucket_offset;
        auto joined_range_bucket_iter = joined_range_bucket_hash_blocks.lower_bound(lower_bound);
        if (joined_range_bucket_iter == joined_range_bucket_hash_blocks.end())
            /// no joined data
            continue;

        bucket_offset = joining_data->join_stop_bucket_offset;
        if constexpr (!is_left_block)
            bucket_offset = joining_data->join_start_bucket_offset;
        auto upper_bound = joining_bucket + bucket_offset;

        for (auto joined_range_bucket_end = joined_range_bucket_hash_blocks.end(); joined_range_bucket_iter != joined_range_bucket_end;
             ++joined_range_bucket_iter)
        {
            if (joined_range_bucket_iter->first > upper_bound)
                /// Reaching the upper bound of right bucket to join
                break;

            auto & joined_bucket_blocks = joined_range_bucket_iter->second;

            /// Although we are bucketing blocks, but the real min/max in 2 buckets may not be join-able
            /// If [min, max] of the joining bucket doesn't intersect with joined range bucket as a whole,
            /// we are sure there will be no join-able rows for the whole bucket
            bool has_intersect = false;
            if constexpr (is_left_block)
                has_intersect = joining_data->intersect(
                    bucket_block.block.minTimestamp(),
                    bucket_block.block.maxTimestamp(),
                    joined_bucket_blocks->blocks.minTimestamp(),
                    joined_bucket_blocks->blocks.maxTimestamp());
            else
                has_intersect = joining_data->intersect(
                    joined_bucket_blocks->blocks.minTimestamp(),
                    joined_bucket_blocks->blocks.maxTimestamp(),
                    bucket_block.block.minTimestamp(),
                    bucket_block.block.maxTimestamp());

            if (!has_intersect)
            {
                ++join_metrics.left_block_and_right_range_bucket_no_intersection_skip;
                continue;
            }

            auto join_block = bucket_block.block; /// We will need make a copy since bucket_block.block can join several buckets
            doJoinBlockWithHashTable<is_left_block>(join_block, joined_bucket_blocks);

            if (join_block.rows())
                joined_blocks.emplace_back(std::move(join_block));
        }
    }

    calculateWatermark();

    left_data.buffered_data->removeOldBuckets("left_stream");
    right_data.buffered_data->removeOldBuckets("right_stream");

    return joined_blocks;
}

std::vector<Block> HashJoin::insertLeftBlockToRangeBucketsAndJoin(Block left_block)
{
    auto joined_blocks = insertBlockToRangeBucketsAndJoin<true>(std::move(left_block));
    for (auto & block : joined_blocks)
        transformToOutputBlock<true>(block);

    return joined_blocks;
}

std::vector<Block> HashJoin::insertRightBlockToRangeBucketsAndJoin(Block right_block)
{
    assert(output_header);

    auto joined_blocks = insertBlockToRangeBucketsAndJoin<false>(std::move(right_block));

    for (auto & block : joined_blocks)
        transformToOutputBlock<false>(block);

    return joined_blocks;
}

bool HashJoin::isRetractBlock(const Block & block, const JoinStreamDescription & join_stream_desc)
{
    if (!join_stream_desc.hasDeltaColumn())
        return false;

    assert(!Streaming::isAppendDataStream(join_stream_desc.data_stream_semantic));
    const auto & delta_column = block.getByPosition(*join_stream_desc.delta_column_position);
    return delta_column.column->getInt(0) < 0;
}

template <bool is_left_block>
std::optional<Block> HashJoin::eraseExistingKeysAndRetractJoin(Block & block)
{
    assert(bidirectional_hash_join);

    JoinData * joining_data = &left_data;
    JoinData * joined_data = &right_data;

    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    if (!isRetractBlock(block, *joining_data->join_stream_desc))
        return {};

    /// First erase the keys
    /// FIXME, avoid the copy
    Block copy_block = block;
    eraseExistingKeys<is_left_block>(copy_block, *joining_data);

    /// Then do retract join
    doJoinBlockWithHashTable<is_left_block>(block, joined_data->buffered_data->getCurrentHashBlocksPtr());

    transformToOutputBlock<is_left_block>(block);

    /// Even empty block, we will need move back to indicate this is a retract block and we have processed it
    return std::move(block);
}

template <bool is_left_block>
void HashJoin::eraseExistingKeys(Block & block, JoinData & join_data)
{
    /// For LATEST / ASOF join, we don't need retract, just drop the retract block on the floor
    /// Actually, it is better to avoid the ChangelogTransform entirely for versioned-kv case.
    /// One challenge to drop the ChangelogTransform for versioned-kv is we will need first
    /// evaluate the whole join semantic first in InterpreterSelectQuery. For multiple join,
    /// it would be a bit difficult
    if (streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Latest)
        return;

    /// Find previous key / values on join columns
    ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(
        block, table_join->getAllNames(is_left_block ? JoinTableSide::Left : JoinTableSide::Right));

    /// FIXME, multiple disjunct OR clause
    const auto & on_expr = table_join->getClauses().front();

    ColumnRawPtrs key_columns;

    const Names * key_names = nullptr;
    if constexpr (is_left_block)
        key_names = &on_expr.key_names_left;
    else
        key_names = &on_expr.key_names_right;

    key_columns.reserve(key_names->size());
    for (const auto & name : *key_names)
        key_columns.push_back(all_key_columns[name]);

    /// FIXME, null map

    Block saved_block = prepareBlock<is_left_block>(block);
    assert(join_data.buffered_data->reserved_column_positions);

    auto & map_variant = join_data.buffered_data->getCurrentHashBlocksPtr()->maps->map_variants[0];
    const auto & key_size = key_sizes[0];
    auto delete_key = isChangelogKeyedStorage(join_data.join_stream_desc->data_stream_semantic);

    joinDispatch(streaming_kind, streaming_strictness, map_variant, [&, this](auto, auto, auto & maps) {
        switch (hash_method_type)
        {
#define M(TYPE) \
    case HashType::TYPE: { \
        using KeyGetter = typename KeyGetterForType<HashType::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type; \
        doEraseExistingKeys<KeyGetter>( \
            *maps.TYPE, saved_block, std::move(key_columns), key_size, *join_data.buffered_data->reserved_column_positions, delete_key); \
        break; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }
    });
}

template <typename KeyGetter, typename Map>
void HashJoin::doEraseExistingKeys(
    Map & map,
    const DB::Block & saved_block,
    ColumnRawPtrs && key_columns,
    const Sizes & key_size,
    const std::vector<size_t> & skip_columns,
    bool delete_key)
{
    [[maybe_unused]] constexpr bool mapped_multiple = std::is_same_v<typename Map::mapped_type, typename MapsMultiple::MappedType>;

    KeyGetter key_getter(std::move(key_columns), key_size, nullptr);

    Arena pool;

    for (size_t i = 0, rows = saved_block.rows(); i < rows; ++i)
    {
        if constexpr (mapped_multiple)
        {
            auto find_result = key_getter.findKey(map, i, pool);
            if (find_result.isFound())
            {
                /// Loop the value list to find a match on other values
                bool found = false;
                auto & mapped = find_result.getMapped();

                for (auto iter = mapped->rows.begin(), iter_end = mapped->rows.end(); iter != iter_end; ++iter)
                {
                    const auto & indexed_block = iter->block_iter->block;
                    /// Compare each column except the delta column in the indexed block with retract block
                    if (compareAt(i, iter->row_num, saved_block.getColumns(), indexed_block.getColumns(), skip_columns) == 0)
                    {
                        mapped->rows.erase(iter);
                        found = true;
                        break;
                    }
                }

                if (!found)
                    /// Key not exist. Out of order
                    throw Exception(ErrorCodes::INTERNAL_ERROR, "Existing value is not found in joined hash table");

                if (mapped->rows.empty() && delete_key)
                {
                    using T = typename Map::mapped_type;
                    /// Release the unique_ptr
                    mapped = T();

                    /// FIXME, erase can be pretty expensive
                    key_getter.eraseKey(map, i, pool);
                }
            }
            else
                /// Key not exist. Out of order
                throw Exception(ErrorCodes::INTERNAL_ERROR, "Existing key is not found in joined hash table");
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error. Expecting hash value to point to multiple values");
    }
}

std::vector<HashJoin::RefListMultipleRef *> HashJoin::eraseOrAppendForPartialPrimaryKeyJoin(const Block & block)
{
    if (!right_data.primary_key_hash_table)
        return {};

    ColumnRawPtrs primary_key_columns;
    primary_key_columns.reserve(right_data.join_stream_desc->primary_key_column_positions->size());

    for (auto col_pos : right_data.join_stream_desc->primary_key_column_positions.value())
        primary_key_columns.push_back(block.getByPosition(col_pos).column.get());

    switch (right_data.primary_key_hash_table->hash_method_type)
    {
#define M(TYPE) \
    case HashType::TYPE: { \
        using KeyGetter = \
            typename KeyGetterForType<HashType::TYPE, std::remove_reference_t<decltype(*right_data.primary_key_hash_table->map.TYPE)>>:: \
                Type; \
        return eraseOrAppendForPartialPrimaryKeyJoin<KeyGetter>( \
            *right_data.primary_key_hash_table->map.TYPE, std::move(primary_key_columns)); \
    }
        APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
    }
}

template <typename KeyGetter, typename Map>
std::vector<HashJoin::RefListMultipleRef *> HashJoin::eraseOrAppendForPartialPrimaryKeyJoin(Map & map, ColumnRawPtrs && primary_key_columns)
{
    auto rows = primary_key_columns[0]->size();
    auto key_getter = KeyGetter(std::move(primary_key_columns), right_data.primary_key_hash_table->key_size, nullptr);

    std::vector<HashJoin::RefListMultipleRef *> multi_refs;
    multi_refs.reserve(rows);

    /// Compact rows since there may be multiple rows which has the same primary key, in this case we only keep the
    /// last one in this block
    std::unordered_map<HashJoin::RefListMultipleRef *, size_t> compacted_multi_refs;
    compacted_multi_refs.reserve(rows);

    Arena lookup_pool;

    for (size_t i = 0; i < rows; ++i)
    {
        /// FIXME, null column
        auto find_result = key_getter.findKey(map, i, lookup_pool);
        std::remove_reference_t<decltype(find_result.getMapped())> * mapped;

        if (find_result.isFound())
        {
            mapped = &find_result.getMapped();

            /// We have seen this primary key before
            /// Erase it from the target linked list since we will append this element soon
            /// Erase followed by append acts like override
            (*mapped)->erase();
        }
        else
        {
            /// This row contains a new primary key,
            /// init it with empty ref
            auto emplace_result = key_getter.emplaceKey(map, i, right_data.primary_key_hash_table->pool);
            mapped = &emplace_result.getMapped();
            mapped = new (mapped) typename Map::mapped_type(std::make_unique<HashJoin::RefListMultipleRef>());
        }

        auto iter = compacted_multi_refs.find(mapped->get());
        if (iter != compacted_multi_refs.end())
            /// Found dup key, null the previous slot and we will skip the previous row
            /// when insert
            multi_refs[iter->second] = nullptr;

        multi_refs.push_back(mapped->get());
        compacted_multi_refs[mapped->get()] = i;
    }

    assert(multi_refs.size() == rows);

    return multi_refs;
}

template <bool is_left_block>
void HashJoin::transformToOutputBlock(Block & joined_block) const
{
    /// Please note we didn't reorder columns according to output header if block is empty to save some cpu cycles
    /// Caller shall check if the retracted block is empty and avoid pushing this empty block downstream since
    /// this empty block's structure probably doesn't match the output header
    if (!joined_block.rows())
        return;

    if constexpr (is_left_block)
    {
        /// For exmaple: c1(i, v, _tp_delta) join c2(i, v, _tp_delta)
        /// (left join right)
        /// joined block:   "i, v, _tp_delta, c2.i, c2.v, c2._tp_delta"
        if (emitChangeLog())
        {
            /// output header:  "i, v, c2.i, c2.v, _tp_delta"
            assert(left_delta_column_position_lrj && right_delta_column_position_lrj);

            /// At first, remove right delta column (from back to front)
            assert(*right_delta_column_position_lrj > *left_delta_column_position_lrj);
            joined_block.erase(*right_delta_column_position_lrj);

            /// Then move left delta column to back
            auto left_delta_col = std::move(joined_block.getByPosition(*left_delta_column_position_lrj));
            joined_block.erase(*left_delta_column_position_lrj);
            joined_block.insert(std::move(left_delta_col));
        }
        else
        {
            /// output header:  "i, v, c2.i, c2.v"
            if (right_delta_column_position_lrj)
                joined_block.erase(*right_delta_column_position_lrj);

            if (left_delta_column_position_lrj)
                joined_block.erase(*left_delta_column_position_lrj);
        }
    }
    else
    {
        /// For exmaple: c1(i, v, _tp_delta) join c2(i, v, _tp_delta)
        /// (right join left)
        /// joined block:   "c2.i, c2.v, c2._tp_delta, i, v, _tp_delta"

        /// Fix the delta column by swapping since the right block has the retract value but got renamed
        if (emitChangeLog())
        {
            /// output header:  "i, v, c2.i, c2.v, _tp_delta"
            assert(left_delta_column_position_rlj && right_delta_column_position_rlj);

            /// At first, move right delta column to left delta column
            auto & right_retract_delta_col = joined_block.getByPosition(*right_delta_column_position_rlj);
            auto & left_delta_col = joined_block.getByPosition(*left_delta_column_position_rlj);
            left_delta_col.column = std::move(right_retract_delta_col.column);

            /// Then remove right delta column, skip this operation since next reordering will ignore it
            // joined_block.erase(*right_delta_column_position_rlj);
        }
        else
        {
            /// output header:  "c2.i, c2.v, i, v"
            /// Skip this operation since next reordering will ignore it
            // if (right_delta_column_position_lrj)
            //     joined_block.erase(*right_delta_column_position_lrj);

            // if (left_delta_column_position_lrj)
            //     joined_block.erase(*left_delta_column_position_lrj);
        }

        joined_block.reorderColumnsInplace(output_header);
    }
}

void HashJoin::calculateWatermark()
{
    Int64 left_watermark = left_data.buffered_data->current_watermark;
    Int64 right_watermark = right_data.buffered_data->current_watermark;

    if (left_watermark == INVALID_WATERMARK || right_watermark == INVALID_WATERMARK)
        return;

    Int64 last_combined_watermark = combined_watermark;
    auto new_combined_watermark = std::min(left_watermark, right_watermark);

    if (last_combined_watermark != new_combined_watermark)
    {
        combined_watermark = new_combined_watermark;

        LOG_INFO(
            logger,
            "Progress combined_watermark={} from last_combined_watermark={} (left_watermark={}, right_watermark={})",
            new_combined_watermark,
            last_combined_watermark,
            left_watermark,
            right_watermark);
    }
}

template <typename KeyGetter, typename Map>
void doRetract(
    const Block & result_block,
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<Map *> & mapv,
    HashJoin::JoinResults & join_results,
    Block & retracted_block)
{
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, typename HashJoin::MapsOne::MappedType>;

    auto disjuncts = mapv.size();
    for (size_t i = 0, rows = result_block.rows(); i < rows; ++i)
    {
        for (size_t on_expr_idx = 0; on_expr_idx < disjuncts; ++on_expr_idx)
        {
            if constexpr (mapped_one)
            {
                /// FIXME, for multiple disjuncts, it's wrong, we will remember which clause fulfills the join
                auto emplace_result = key_getter_vector[on_expr_idx].emplaceKey(*(mapv[on_expr_idx]), i, join_results.pool);
                if (emplace_result.isInserted())
                {
                    /// New key item
                    new (&emplace_result.getMapped()) typename Map::mapped_type(&join_results.blocks, i);
                }
                else
                {
                    /// Retract the previous joined row
                    auto & mapped = emplace_result.getMapped();

                    insertRow(retracted_block, mapped.row_num, mapped.block_iter->block.getColumns());

                    /// We need explicitly destroy for RowRefWithRefCount case
                    /// Then we can do proper garbage collection
                    using T = typename Map::mapped_type;
                    mapped.~T();

                    new (&emplace_result.getMapped()) typename Map::mapped_type(&join_results.blocks, i);
                }
            }
            else
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Retraction is not supported for the current join type");
            }
        }
    }
}

Block HashJoin::retract(const Block & result_block)
{
    assert(result_block.rows());

    assert(join_results);

    Block retracted_block = result_block.cloneEmpty();

    std::scoped_lock lock(join_results->mutex);

    /// First, buffer the new join results
    join_results->blocks.pushBack(Block(result_block));

    const auto & on_exprs = table_join->getClauses();
    auto disjuncts = on_exprs.size();

    /// Second, Build hash index for join results and in the meanwhile build retract block
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(on_exprs.size());
    for (size_t i = 0; i < disjuncts; ++i)
        join_on_keys.emplace_back(result_block, on_exprs[i].key_names_left, cond_column_names[i].first, key_sizes[i]);

    using MapsVariantType = std::decay_t<decltype(join_results->maps->map_variants[0])>;
    std::vector<MapsVariantType *> maps_vector;
    maps_vector.reserve(disjuncts);

    for (size_t i = 0; i < disjuncts; ++i)
        maps_vector.push_back(&join_results->maps->map_variants[i]);

    joinDispatch(streaming_kind, streaming_strictness, maps_vector, [&, this](auto, auto, auto & mapv) {
        /// maps_vector_ => std::vector<Maps *>
        using ElemntType = std::decay_t<decltype(*mapv.begin())>;
        using Maps = std::remove_pointer_t<ElemntType>;

        switch (hash_method_type)
        {
#define M(TYPE) \
    case HashType::TYPE: { \
        using MapTypeVal = typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<HashType::TYPE, MapTypeVal>::Type; \
        std::vector<MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        key_getter_vector.reserve(disjuncts); \
        for (size_t d = 0; d < disjuncts; ++d) \
        { \
            auto & join_on_key = join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.emplace_back(std::move(join_on_key.key_columns), join_on_key.key_sizes, nullptr); \
        } \
        doRetract(result_block, std::move(key_getter_vector), a_map_type_vector, *join_results, retracted_block); \
        break; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS(M)
#undef M
        }
    });

    if (retracted_block.rows())
    {
        /// Update _tp_delta from 1 => -1
        /// FIXME, calculate _tp_delta position in retracted block
        auto & col = retracted_block.getByName(ProtonConsts::RESERVED_DELTA_FLAG);
        auto & data = assert_cast<ColumnInt8 &>(col.column->assumeMutableRef()).getData();
        std::fill(data.begin(), data.end(), -1);

        /// col.column = col.type->createColumnConst(retracted_block.rows(), -1);
    }
    return retracted_block;
}

void HashJoin::reviseJoinStrictness()
{
    /// Strictness::Any in streaming join means take the latest row to join
    any_take_last_row = (streaming_strictness == Strictness::Latest);

    /// For explicit asof / latest join, we honor what end users like to have
    if (streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Latest)
        return;

    if (streaming_strictness == Strictness::Range)
    {
        if (!isAppendDataStream(right_data.join_stream_desc->data_stream_semantic))
            throw Exception(ErrorCodes::UNSUPPORTED, "Range join keyed stream or changelog stream is not supported");

        return;
    }

    /// We don't even care the left stream semantic since if right stream is versioned-kv or changelog-kv
    /// we will use `Multiple` list to hold the values since users may use partial primary key to join
    if (!isAppendDataStream(right_data.join_stream_desc->data_stream_semantic))
        streaming_strictness = Streaming::Strictness::Multiple;
}

void HashJoin::checkLimits() const
{
    const auto & left_total_bytes = left_data.buffered_data->getJoinMetrics().totalBytes();
    const auto & right_total_bytes = right_data.buffered_data->getJoinMetrics().totalBytes();
    auto current_total_bytes = left_total_bytes + right_total_bytes;
    if (current_total_bytes >= join_max_cached_bytes)
        throw Exception(
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED,
            "Streaming join's memory reaches max size: {}, current total: {}, left total: {}, right total: {}",
            join_max_cached_bytes,
            current_total_bytes,
            left_total_bytes,
            right_total_bytes);
}

void HashJoin::validateAsofJoinKey()
{
    if (streaming_strictness != Strictness::Range)
        return;

    if (table_join->rangeAsofJoinContext().type != RangeType::Interval)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Only time interval range join is supported in stream to stream join. Use `date_diff(...) or date_diff_within(...) function.");

    const auto & join_clause = table_join->getOnlyClause();

    auto check_type = [](const Block & header, size_t col_pos) {
        auto type = header.getByPosition(col_pos).type;
        if (!isDateTime64(type) && !isDateTime(type))
            throw DB::Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The range column in stream to stream join only supports datetime64 or datetime column types");
    };

    auto left_asof_col_pos = left_data.join_stream_desc->input_header.getPositionByName(join_clause.key_names_left.back());
    auto right_asof_col_pos = right_data.join_stream_desc->input_header.getPositionByName(join_clause.key_names_right.back());

    check_type(left_data.join_stream_desc->input_header, left_asof_col_pos);
    check_type(right_data.join_stream_desc->input_header, right_asof_col_pos);

    const auto & left_asof_col_with_type = left_data.join_stream_desc->input_header.getByPosition(left_asof_col_pos);
    const auto & right_asof_col_with_type = right_data.join_stream_desc->input_header.getByPosition(right_asof_col_pos);

    if (left_asof_col_with_type.type->getTypeId() != right_asof_col_with_type.type->getTypeId())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The columns in stream to stream range join have different column types");

    UInt32 scale = 0;
    if (isDateTime64(left_asof_col_with_type.type))
    {
        /// Check scale
        const auto * left_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(left_asof_col_with_type.type.get());
        assert(left_datetime64_type);

        const auto * right_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(right_asof_col_with_type.type.get());
        assert(right_datetime64_type);

        if (left_datetime64_type->getScale() != right_datetime64_type->getScale())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The datetime64 columns in stream to stream range join have different scales");

        scale = left_datetime64_type->getScale();
    }

    left_data.buffered_data->updateAsofJoinColumnPositionAndScale(scale, left_asof_col_pos, left_asof_col_with_type.type->getTypeId());
    right_data.buffered_data->updateAsofJoinColumnPositionAndScale(scale, right_asof_col_pos, left_asof_col_with_type.type->getTypeId());
}

void HashJoin::checkJoinSemantic() const
{
    if (!table_join->oneDisjunct())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream to Stream join only supports only one disjunct join clause");

    /// For streaming join, we don't support inline JOIN ON predicate like `left.value > 10` in the following query example
    /// since stream query will need buffer more additional non-joined data in-memory
    /// SELECT * FROM left INNER JOIN right ON left.key = right.key AND left.value > 10 and right.value > 80;
    /// User shall use WHERE predicate like
    /// SELECT * FROM (SELECT * FROM left WHERE left.value > 10) as left INNER JOIN right ON left.key = right.key
    for (const auto & on_expr : table_join->getClauses())
    {
        const auto & on_expr_cond_column_names = on_expr.condColumnNames();
        if (!on_expr_cond_column_names.first.empty() || !on_expr_cond_column_names.second.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Streaming join doesn't support predicates in JOIN ON clause. Use WHERE predicate instead");
    }

    validate(
        {left_data.join_stream_desc->data_stream_semantic.toStorageSemantic(),
         kind,
         strictness,
         right_data.join_stream_desc->data_stream_semantic.toStorageSemantic()});
}

HashMapSizes HashJoin::sizesOfMapsVariant(const MapsVariant & maps_variant) const
{
    HashMapSizes sizes;
    joinDispatch(streaming_kind, streaming_strictness, maps_variant, [&](auto /*kind_*/, auto /*strictness_*/, auto & maps) {
        sizes.keys = maps.getTotalRowCount();
        sizes.buffer_size_in_bytes = maps.getBufferSizeInBytes();
        sizes.buffer_bytes_in_cells = maps.getBufferSizeInCells();
    });
    return sizes;
}

HashJoin::PrimaryKeyHashTable::PrimaryKeyHashTable(HashType hash_method_type_, Sizes && key_size_)
    : hash_method_type(hash_method_type_), key_size(std::move(key_size_))
{
    map.create(hash_method_type);
}

String HashJoin::JoinResults::joinMetricsString(const HashJoin * join) const
{
    size_t total_blocks_cached = blocks.size();
    size_t total_blocks_bytes_cached = 0;
    size_t total_blocks_allocated_bytes_cached = 0;
    size_t total_blocks_rows_cached = 0;
    size_t total_arena_bytes = 0;
    size_t total_arena_chunks = 0;

    for (const auto & block : blocks)
    {
        total_blocks_rows_cached += block.block.rows();
        total_blocks_bytes_cached += block.block.allocatedBytes();
        total_blocks_allocated_bytes_cached += block.block.allocatedBytes();
    }

    total_arena_bytes += pool.size();
    total_arena_chunks += pool.numOfChunks();

    HashMapSizes sizes;
    if (maps)
        sizes = maps->sizes(join);

    return fmt::format(
        "total_blocks_cached={}, total_blocks_bytes_cached={}, total_blocks_allocated_bytes_cached={}, "
        "total_blocks_rows_cached={}, total_arena_bytes={}, total_arena_chunks={}, {{{}}} recorded_join_metrics={{{}}}",
        total_blocks_cached,
        total_blocks_bytes_cached,
        total_blocks_allocated_bytes_cached,
        total_blocks_rows_cached,
        total_arena_bytes,
        total_arena_chunks,
        sizes.string(),
        metrics.string());
}

HashMapSizes HashJoinMapsVariants::sizes(const HashJoin * join) const
{
    HashMapSizes sizes;
    for (const auto & maps : map_variants)
    {
        auto one_sizes = join->sizesOfMapsVariant(maps);
        sizes.keys += one_sizes.keys;
        sizes.buffer_size_in_bytes += one_sizes.buffer_size_in_bytes;
        sizes.buffer_bytes_in_cells += one_sizes.buffer_bytes_in_cells;
    }

    return sizes;
}

void HashJoin::serialize(WriteBuffer & wb, VersionType version) const
{
    /// Part-1: ON clauses
    DB::writeStringBinary(TableJoin::formatClauses(table_join->getClauses(), true), wb);

    /// Part-2: Description of left/right join stream
    DB::writeStringBinary(left_data.join_stream_desc->input_header.dumpStructure(), wb);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(left_data.join_stream_desc->data_stream_semantic.semantic), wb);
    DB::writeIntBinary(left_data.join_stream_desc->keep_versions, wb);

    DB::writeStringBinary(right_data.join_stream_desc->input_header.dumpStructure(), wb);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(right_data.join_stream_desc->data_stream_semantic.semantic), wb);
    DB::writeIntBinary(right_data.join_stream_desc->keep_versions, wb);

    /// Part-3: Join method
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(streaming_kind), wb);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(streaming_strictness), wb);
    // DB::writeBinary(key_sizes, wb); /// No need after serialized `ON clauses` and left/right streams's header
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(hash_method_type), wb);

    /// Part-4: Buffered data of left/right join stream
    if (bidirectional_hash_join)
        left_data.serialize(wb, version);

    right_data.serialize(wb, version);

    /// Part-5: Asof type (Optional)
    bool need_asof = streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof;
    if (need_asof)
    {
        assert(asof_type.has_value());
        DB::writeIntBinary<UInt16>(static_cast<UInt16>(*asof_type), wb);
        DB::writeIntBinary<UInt16>(static_cast<UInt16>(asof_inequality), wb);
    }

    /// Part-6: Emit changelog (Optional)
    DB::writeBoolText(join_results.has_value(), wb);
    if (join_results.has_value())
    {
        assert(retract_push_down && emit_changelog);
        join_results->serialize(wb, version, *this);
    }

    /// Part-7: Others
    DB::writeIntBinary(combined_watermark.load(), wb);
    join_metrics.serialize(wb, version);
}

void HashJoin::deserialize(ReadBuffer & rb, VersionType version)
{
    { /// Part-1: ON clauses
        auto clauses_str = TableJoin::formatClauses(table_join->getClauses(), true);
        String recovered_clauses_str;
        DB::readStringBinary(recovered_clauses_str, rb);
        if (recovered_clauses_str != clauses_str)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The ON clauses of join are not the same, checkpointed={}, current={}",
                recovered_clauses_str,
                clauses_str);
    }

    { /// Part-2: Description of left/right join stream
        String recovered_left_header_str;
        UInt16 recovered_left_stream_semantic;
        UInt64 recovered_left_keep_versions;
        DB::readStringBinary(recovered_left_header_str, rb);
        DB::readIntBinary<UInt16>(recovered_left_stream_semantic, rb);
        DB::readIntBinary(recovered_left_keep_versions, rb);

        auto left_header_str = left_data.join_stream_desc->input_header.dumpStructure();
        if (recovered_left_header_str != left_header_str
            || static_cast<DataStreamSemantic>(recovered_left_stream_semantic) != left_data.join_stream_desc->data_stream_semantic.semantic
            || recovered_left_keep_versions != left_data.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The description of left join stream are not the same, checkpointed: header={}, "
                "semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_left_header_str,
                static_cast<DataStreamSemantic>(recovered_left_stream_semantic),
                recovered_left_keep_versions,
                left_header_str,
                left_data.join_stream_desc->data_stream_semantic.semantic,
                left_data.join_stream_desc->keep_versions);

        String recovered_right_header_str;
        UInt16 recovered_right_stream_semantic;
        UInt64 recovered_right_keep_versions;
        DB::readStringBinary(recovered_right_header_str, rb);
        DB::readIntBinary<UInt16>(recovered_right_stream_semantic, rb);
        DB::readIntBinary(recovered_right_keep_versions, rb);

        auto right_header_str = right_data.join_stream_desc->input_header.dumpStructure();
        if (recovered_right_header_str != right_header_str
            || static_cast<DataStreamSemantic>(recovered_right_stream_semantic)
                != right_data.join_stream_desc->data_stream_semantic.semantic
            || recovered_right_keep_versions != right_data.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The description of right join stream are not the same, checkpointed: header={}, "
                "semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_right_header_str,
                static_cast<DataStreamSemantic>(recovered_right_stream_semantic),
                recovered_right_keep_versions,
                right_header_str,
                right_data.join_stream_desc->data_stream_semantic.semantic,
                right_data.join_stream_desc->keep_versions);
    }

    { /// Part-3: Join method
        UInt16 recovered_streaming_kind;
        DB::readIntBinary<UInt16>(recovered_streaming_kind, rb);
        if (static_cast<Kind>(recovered_streaming_kind) != streaming_kind)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The kind of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(static_cast<Kind>(recovered_streaming_kind)),
                magic_enum::enum_name(streaming_kind));

        UInt16 recovered_streaming_strictness;
        DB::readIntBinary<UInt16>(recovered_streaming_strictness, rb);
        if (static_cast<Strictness>(recovered_streaming_strictness) != streaming_strictness)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The strictness of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(static_cast<Strictness>(recovered_streaming_strictness)),
                magic_enum::enum_name(streaming_strictness));

        UInt16 recovered_hash_method_type;
        DB::readIntBinary<UInt16>(recovered_hash_method_type, rb);
        if (static_cast<HashType>(recovered_hash_method_type) != hash_method_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The hash method type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(static_cast<HashType>(recovered_hash_method_type)),
                magic_enum::enum_name(hash_method_type));
    }

    /// Part-4: Buffered data of left/right join stream
    if (bidirectional_hash_join)
        left_data.deserialize(rb, version);

    right_data.deserialize(rb, version);

    /// Part-5: Asof type (Optional)
    bool need_asof = streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof;
    if (need_asof)
    {
        assert(asof_type.has_value());
        UInt16 recovered_asof_type;
        DB::readIntBinary<UInt16>(recovered_asof_type, rb);
        if (static_cast<TypeIndex>(recovered_asof_type) != asof_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The asof type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(static_cast<TypeIndex>(recovered_asof_type)),
                magic_enum::enum_name(*asof_type));

        UInt16 recovered_asof_inequality;
        DB::readIntBinary<UInt16>(recovered_asof_inequality, rb);
        if (static_cast<ASOFJoinInequality>(recovered_asof_inequality) != asof_inequality)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The asof inequality of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(static_cast<ASOFJoinInequality>(recovered_asof_inequality)),
                magic_enum::enum_name(asof_inequality));
    }

    /// Part-6: Emit changelog (Optional)
    bool need_emit_changelog;
    DB::readBoolText(need_emit_changelog, rb);
    if (need_emit_changelog)
    {
        if (!join_results.has_value())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hash join checkpoint. The emit changelog strategy of join are not the same, checkpointed={}, current={}",
                need_emit_changelog,
                join_results.has_value());

        assert(retract_push_down && emit_changelog);
        join_results->deserialize(rb, version, *this);
    }

    /// Part-7: Others
    int64_t recovered_combined_watermark;
    DB::readIntBinary(recovered_combined_watermark, rb);
    combined_watermark = recovered_combined_watermark;

    join_metrics.deserialize(rb, version);
}

void HashJoin::JoinResults::serialize(WriteBuffer & wb, VersionType version, const HashJoin & join) const
{
    std::scoped_lock lock(mutex);
    assert(maps);
    serializeHashJoinMapsVariants(blocks, *maps, wb, version, sample_block, join);

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.serialize(wb, version);
}

void HashJoin::JoinResults::deserialize(ReadBuffer & rb, VersionType version, const HashJoin & join)
{
    std::scoped_lock lock(mutex);
    assert(maps);
    deserializeHashJoinMapsVariants(blocks, *maps, rb, version, pool, sample_block, join);

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.deserialize(rb, version);
}

void HashJoin::JoinData::serialize(WriteBuffer & wb, VersionType version) const
{
    bool has_data = static_cast<bool>(buffered_data);
    writeBoolText(has_data, wb);
    if (!has_data)
        return;

    bool has_primary_key_hash_table = static_cast<bool>(primary_key_hash_table);
    writeBoolText(has_primary_key_hash_table, wb);
    if (has_primary_key_hash_table)
    {
        SerializedRowRefListMultipleToIndices serialized_row_ref_list_multiple_to_indices;
        buffered_data->serialize(wb, version, &serialized_row_ref_list_multiple_to_indices);

        primary_key_hash_table->map.serialize(
            /*MappedSerializer*/
            [&](const RowRefListMultipleRefPtr<JoinDataBlock> & mapped, WriteBuffer & wb_) {
                mapped->serialize(serialized_row_ref_list_multiple_to_indices, wb_);
            },
            wb);
    }
    else
        buffered_data->serialize(wb, version, nullptr);
}

void HashJoin::JoinData::deserialize(ReadBuffer & rb, VersionType version)
{
    bool has_data = static_cast<bool>(buffered_data);
    bool recovered_has_data;
    readBoolText(recovered_has_data, rb);
    if (recovered_has_data != has_data)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The data of hash join are not the same, checkpointed={}, current={}",
            recovered_has_data ? "Has JoinData" : "No JoinData",
            has_data ? "Has JoinData" : "No JoinData");

    if (!has_data)
        return;

    bool has_primary_key_hash_table = static_cast<bool>(primary_key_hash_table);
    bool recovered_has_primary_key_hash_table;
    readBoolText(recovered_has_primary_key_hash_table, rb);
    if (recovered_has_primary_key_hash_table != has_primary_key_hash_table)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The primary key hash table of hash join are not the same, checkpointed={}, current={}",
            recovered_has_primary_key_hash_table ? "Has PrimaryKeyHashTable" : "No PrimaryKeyHashTable",
            has_primary_key_hash_table ? "Has PrimaryKeyHashTable" : "No PrimaryKeyHashTable");

    if (has_primary_key_hash_table)
    {
        DeserializedIndicesToRowRefListMultiple<JoinDataBlock> deserialized_indices_to_multiple_ref;
        buffered_data->deserialize(rb, version, &deserialized_indices_to_multiple_ref);

        primary_key_hash_table->map.deserialize(
            /*MappedDeserializer*/
            [&](RowRefListMultipleRefPtr<JoinDataBlock> & mapped, Arena &, ReadBuffer & rb_) {
                mapped = std::make_unique<RowRefListMultipleRef<JoinDataBlock>>();
                mapped->deserialize(deserialized_indices_to_multiple_ref, rb_);
            },
            primary_key_hash_table->pool,
            rb);
    }
    else
        buffered_data->deserialize(rb, version, nullptr);
}

void HashJoin::JoinGlobalMetrics::serialize(WriteBuffer & wb, VersionType) const
{
    DB::writeBinary(total_join, wb);
    DB::writeBinary(left_block_and_right_range_bucket_no_intersection_skip, wb);
    DB::writeBinary(right_block_and_left_range_bucket_no_intersection_skip, wb);
}

void HashJoin::JoinGlobalMetrics::deserialize(ReadBuffer & rb, VersionType)
{
    DB::readBinary(total_join, rb);
    DB::readBinary(left_block_and_right_range_bucket_no_intersection_skip, rb);
    DB::readBinary(right_block_and_left_range_bucket_no_intersection_skip, rb);
}

void serializeHashJoinMapsVariants(
    const JoinDataBlockList & blocks,
    const HashJoinMapsVariants & maps,
    WriteBuffer & wb,
    VersionType version,
    const Block & header,
    const HashJoin & join,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices)
{
    SerializedBlocksToIndices serialized_blocks_to_indices;
    blocks.serialize(wb, version, header, &serialized_blocks_to_indices);

    assert(maps.map_variants.size() >= 1);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(maps.map_variants.size()), wb);

    std::vector<const std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (const auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    auto mapped_serializer = [&](const auto & mapped_, WriteBuffer & wb_) {
        using Mapped = std::decay_t<decltype(mapped_)>;
        if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs<JoinDataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.serialize(*join.getAsofType(), serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, AsofRowRefs<JoinDataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.serialize(*join.getAsofType(), serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<JoinDataBlock>>)
        {
            mapped_.serialize(serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr<JoinDataBlock>>)
        {
            mapped_->serialize(serialized_blocks_to_indices, wb_, serialized_row_ref_list_multiple_to_indices);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefList<JoinDataBlock>>)
        {
            mapped_.serialize(serialized_blocks_to_indices, wb_);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The mapped type {} of hash join map isn't supported", typeid(Mapped).name());
    };

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, const auto & mapv) {
            for (auto * map : mapv)
                map->serialize(mapped_serializer, wb);
        });
}

void deserializeHashJoinMapsVariants(
    JoinDataBlockList & blocks,
    HashJoinMapsVariants & maps,
    ReadBuffer & rb,
    VersionType version,
    Arena & pool,
    const Block & header,
    const HashJoin & join,
    DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref)
{
    DeserializedIndicesToBlocks<JoinDataBlock> deserialized_indices_to_blocks;
    blocks.deserialize(rb, version, header, &deserialized_indices_to_blocks);

    UInt16 maps_size;
    DB::readIntBinary<UInt16>(maps_size, rb);
    if (maps_size != maps.map_variants.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The number of hash join maps varitans are not the same, checkpointed={}, current={}",
            maps_size,
            maps.map_variants.size());
    assert(maps_size >= 1);

    std::vector<std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    auto mapped_deserializer = [&](auto & mapped_, [[maybe_unused]] Arena & pool_, ReadBuffer & rb_) {
        using Mapped = std::decay_t<decltype(mapped_)>;
        if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs<JoinDataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.deserialize(*join.getAsofType(), deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, AsofRowRefs<JoinDataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.deserialize(*join.getAsofType(), &blocks, deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<JoinDataBlock>>)
        {
            mapped_.deserialize(&blocks, deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr<JoinDataBlock>>)
        {
            mapped_ = std::make_unique<RowRefListMultiple<JoinDataBlock>>();
            mapped_->deserialize(&blocks, deserialized_indices_to_blocks, rb_, deserialized_indices_to_multiple_ref);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefList<JoinDataBlock>>)
        {
            mapped_.deserialize(pool_, deserialized_indices_to_blocks, rb_);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The mapped type {} of hash join map isn't supported", typeid(Mapped).name());
    };

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, auto & mapv) {
            for (auto * map : mapv)
                map->deserialize(mapped_deserializer, pool, rb);
        });
}
}
}
