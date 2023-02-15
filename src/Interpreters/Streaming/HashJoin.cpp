#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/joinDispatch.h>
#include <Interpreters/TableJoin.h>
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
}

namespace Streaming
{
namespace
{
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

template <typename Mapped, bool need_offset = false>
using FindResultImpl = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

constexpr bool use_offset = true;

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
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
        const Block & block,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        JoinTupleMap * joined_rows_,
        const RangeAsofJoinContext & range_join_ctx_,
        bool is_asof_join,
        bool is_left_block = true)
        : join_on_keys(std::move(join_on_keys_))
        , rows_to_add(block.rows())
        , joined_rows(joined_rows_)
        , range_join_ctx(range_join_ctx_)
        , src_block_id(block.info.blockID())
        , buffer_left_stream(join.needBufferLeftStream())
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

        for (const auto & src_column : block_with_columns_to_add)
        {
            if (is_left_block)
            {
                /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
                /// because it uses not qualified right block column names
                auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
                /// Don't insert column if it's in left block
                if (!block.has(qualified_name))
                    addColumn(src_column, qualified_name);
            }
            else
            {
                /// This is a right block which means it is bi-directional hash join and
                /// it is right block join left hash table, we don't need rename column name.
                /// So add left column directly
                if (!block.has(src_column.name))
                    addColumn(src_column, src_column.name);
            }
        }

        if (is_asof_join)
        {
            assert(join_on_keys.size() == 1);
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column, right_asof_column.name);
            left_asof_key = join_on_keys[0].key_columns.back();
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }

    template <bool has_defaults>
    void appendFromBlock(const Block & block, size_t row_num)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            columns[j]->insertFrom(*block.getByPosition(right_indexes[j]).column, row_num);
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
    const IColumn & leftAsofKey() const { return *left_asof_key; }

    std::vector<JoinOnKeyColumns> join_on_keys;

    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;

    /// proton : starts
    JoinTupleMap * joined_rows;
    const RangeAsofJoinContext & range_join_ctx;
    UInt64 src_block_id;
    bool buffer_left_stream;
    /// proton : ends

private:
    std::vector<TypeAndName> type_name;
    MutableColumns columns;
    std::vector<size_t> right_indexes;
    size_t lazy_defaults_count = 0;
    /// for ASOF
    std::optional<TypeIndex> asof_type;
    ASOFJoinInequality asof_inequality;
    const IColumn * left_asof_key = nullptr;

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
    static constexpr bool is_any_join = STRICTNESS == Strictness::Any;
    static constexpr bool is_all_join = STRICTNESS == Strictness::All;
    static constexpr bool is_asof_join = STRICTNESS == Strictness::Asof;
    static constexpr bool is_range_asof_join = STRICTNESS == Strictness::RangeAsof;
    static constexpr bool is_range_join = STRICTNESS == Strictness::Range;

    static constexpr bool left = KIND == Kind::Left;
    static constexpr bool right = KIND == Kind::Right;
    static constexpr bool inner = KIND == Kind::Inner;

    static constexpr bool need_replication = is_all_join || (is_any_join && right) || is_range_join;
    static constexpr bool need_filter = !need_replication && (inner || right);
    static constexpr bool add_missing = left;

    static constexpr bool need_flags = MapGetter<KIND, STRICTNESS>::flagged;
};

template <typename Map, bool add_missing>
bool addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added_columns,
    IColumn::Offset & current_offset,
    uint32_t row_num_in_left_block)
{
    if constexpr (add_missing)
        added_columns.applyLazyDefaults();

    bool added = false;
    for (auto it = mapped.begin(); it.ok(); ++it)
    {
        auto result
            = added_columns.joined_rows->insert(JoinTuple{added_columns.src_block_id, it->block, row_num_in_left_block, it->row_num});
        if (result.second)
        {
            /// Is not joined yet
            added_columns.appendFromBlock<false>(*it->block, it->row_num);
            ++current_offset;
            added = true;
        }
    }

    return added;
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

/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map, bool multiple_disjuncts>
NO_INLINE IColumn::Filter
joinRightColumns(std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    assert(added_columns.joined_rows);

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
                    const IColumn & left_asof_key = added_columns.leftAsofKey();

                    if (auto row_refs = mapped.findRange(
                            asof_type,
                            added_columns.range_join_ctx,
                            left_asof_key,
                            i,
                            added_columns.src_block_id,
                            added_columns.joined_rows);
                        !row_refs.empty())
                    {
                        setUsed<need_filter>(filter, i);

                        for (auto & row_ref : row_refs)
                        {
                            added_columns.appendFromBlock<jf.add_missing>(*row_ref.block, row_ref.row_num);
                            ++current_offset;

                            assert(added_columns.joined_rows);
                            [[maybe_unused]] auto result = added_columns.joined_rows->insert(
                                JoinTuple{added_columns.src_block_id, row_ref.block, i, row_ref.row_num});
                            assert(result.second);
                        }
                    }
                    else
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                }
                else if constexpr (jf.is_range_asof_join)
                {
                    /// FIXME
                }
                else if constexpr (jf.is_asof_join)
                {
                    TypeIndex asof_type = added_columns.asofType();
                    ASOFJoinInequality asof_inequality = added_columns.asofInequality();
                    const IColumn & left_asof_key = added_columns.leftAsofKey();

                    if (const auto * found = mapped.findAsof(asof_type, asof_inequality, left_asof_key, i))
                    {
                        assert(added_columns.joined_rows);

                        bool unjoined = true;
                        if (added_columns.buffer_left_stream)
                        {
                            std::tie(std::ignore, unjoined) = added_columns.joined_rows->insert(
                                JoinTuple{added_columns.src_block_id, &found->block_iter->block, i, found->row_num});
                        }

                        /// If there are multiple same asof key elements, the current algorithm can pick the first one
                        /// to join which may be already joined. So far we don't pass joined_rows to further
                        /// filtering in `findAsof(...)` for perf concern.
                        if (unjoined)
                        {
                            setUsed<need_filter>(filter, i);
                            added_columns.appendFromBlock<jf.add_missing>(found->block_iter->block, found->row_num);
                        }
                    }
                    else
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                }
                else if constexpr (jf.is_all_join)
                {
                    if (addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, i))
                        setUsed<need_filter>(filter, i);
                }
                else if constexpr ((jf.is_any_join) && jf.right)
                {
                    /// FIXME
                }
                else if constexpr (jf.is_any_join && KIND == Kind::Inner)
                {
                    assert(!added_columns.buffer_left_stream);
                    setUsed<need_filter>(filter, i);
                    added_columns.appendFromBlock<jf.add_missing>(mapped.block_iter->block, mapped.row_num);
                    break;
                }
                else /// ANY LEFT
                {
                    /// FIXME
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

        if (!right_row_found)
            addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);

        if constexpr (jf.need_replication)
            (*added_columns.offsets_to_replicate)[i] = current_offset;
    }

    added_columns.applyLazyDefaults();
    return filter;
}

template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map>
IColumn::Filter joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    return mapv.size() > 1
        ? joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns)
        : joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
}

template <Kind KIND, Strictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
{
    bool has_null_map
        = std::any_of(added_columns.join_on_keys.begin(), added_columns.join_on_keys.end(), [](const auto & k) { return k.null_map; });
    if (added_columns.need_filter)
    {
        if (has_null_map)
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        else
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
    }
    else
    {
        if (has_null_map)
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        else
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
    }
}

template <Kind KIND, Strictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinRightColumns(const std::vector<const Maps *> & mapv, AddedColumns & added_columns, HashJoin::Type type)
{
    constexpr bool is_asof_join
        = (STRICTNESS == Strictness::Asof || STRICTNESS == Strictness::Range || STRICTNESS == Strictness::RangeAsof);

    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using MapTypeVal = const typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, MapTypeVal>::Type; \
        std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d) \
        { \
            const auto & join_on_key = added_columns.join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.push_back( \
                std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes))); \
        } \
        return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetter>( \
            std::move(key_getter_vector), a_map_type_vector, added_columns); \
    }
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", type);
    }
}

/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <typename Map, typename KeyGetter>
struct Inserter
{
    static ALWAYS_INLINE void insertAll(const HashJoin &, Map & map, KeyGetter & key_getter, const Block * stored_block, size_t i, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        else
            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
            emplace_result.getMapped().insert({stored_block, i}, pool);
    }

    static ALWAYS_INLINE void insertOne(
        const HashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        JoinBlockList * blocks,
        size_t i,
        Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted())
        {
            new (&emplace_result.getMapped()) typename Map::mapped_type(blocks, i);
        }
        else if (join.anyTakeLastRow())
        {
            /// We need explicitly destroy for RowRefWithRefCount case
            /// Then we can do proper garbage collection
            using T = typename Map::mapped_type;
            emplace_result.getMapped().~T();

            /// aggregates_pool->setCurrentTimestamps(window_lower_bound, window_upper_bound);

            new (&emplace_result.getMapped()) typename Map::mapped_type(blocks, i);
        }
    }

    static ALWAYS_INLINE void insertRangeAsof(
        HashJoin & join, Map & map, KeyGetter & key_getter, const Block * stored_block, size_t i, Arena & pool, const IColumn & asof_column)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);
        typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);
        time_series_map->insert(asof_type, asof_column, stored_block, i);
    }

    static ALWAYS_INLINE void insertAsof(
        HashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        JoinBlockList * blocks,
        size_t i,
        Arena & pool,
        const IColumn & asof_column,
        UInt64 keep_versions)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);
        typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);

        time_series_map->insert(asof_type, asof_column, blocks, i, join.getAsofInequality(), keep_versions);
    }
};

template <Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
size_t NO_INLINE insertFromBlockImplTypeCase(
    HashJoin & join,
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    JoinBlockList * blocks,
    ConstNullMapPtr null_map,
    Arena & pool)
{
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRefWithRefCount>;

    constexpr bool is_range_asof_join = (STRICTNESS == Strictness::RangeAsof) || (STRICTNESS == Strictness::Range);
    constexpr bool is_asof_join = STRICTNESS == Strictness::Asof;

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_range_asof_join || is_asof_join)
        asof_column = key_columns.back();

    auto key_getter = createKeyGetter < KeyGetter, is_range_asof_join || is_asof_join > (key_columns, key_sizes);

    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            continue;

        if constexpr (is_range_asof_join)
            Inserter<Map, KeyGetter>::insertRangeAsof(join, map, key_getter, blocks->lastBlock(), i, pool, *asof_column);
        else if constexpr (is_asof_join)
            Inserter<Map, KeyGetter>::insertAsof(
                join, map, key_getter, blocks, i, pool, *asof_column, join.keepVersions());
        else if constexpr (mapped_one)
            Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, blocks, i, pool);
        else
            Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, blocks->lastBlock(), i, pool);
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
    JoinBlockList * blocks,
    ConstNullMapPtr null_map,
    Arena & pool)
{
    if (null_map)
        return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(
            join, map, rows, key_columns, key_sizes, blocks, null_map, pool);
    else
        return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(
            join, map, rows, key_columns, key_sizes, blocks, null_map, pool);
}

template <Strictness STRICTNESS, typename Maps>
size_t insertFromBlockImpl(
    HashJoin & join,
    HashJoin::Type type,
    Maps & maps,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    JoinBlockList * blocks,
    ConstNullMapPtr null_map,
    Arena & pool)
{
    switch (type)
    {
        case HashJoin::Type::EMPTY:
            assert(false);
            return 0;
        case HashJoin::Type::CROSS:
            assert(false);
            return 0; /// Do nothing. We have already saved block, and it is enough.
        case HashJoin::Type::DICT:
            assert(false);
            return 0; /// No one should call it with Type::DICT.

#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return insertFromBlockImplType< \
            STRICTNESS, \
            typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            join, *maps.TYPE, rows, key_columns, key_sizes, blocks, null_map, pool); \
        break;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

inline void addDeltaColumn(Block & block, size_t rows)
{
    /// Add _tp_delta = 1 column to result block
    auto delta_type = DataTypeFactory::instance().get("int8");
    auto delta_col = delta_type->createColumn();
    delta_col->insertMany(1, rows);
    block.insert({std::move(delta_col), std::move(delta_type), ProtonConsts::RESERVED_DELTA_FLAG});
}
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_, JoinStreamDescription left_join_stream_desc_, JoinStreamDescription right_join_stream_desc_)
    : table_join(std::move(table_join_))
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(false)
    , asof_inequality(table_join->getAsofInequality())
    , left_stream_desc(std::move(left_join_stream_desc_))
    , right_stream_desc(std::move(right_join_stream_desc_))
    , log(&Poco::Logger::get("StreamingHashJoin"))
{
    checkJoinSemantic();

    streaming_kind = Streaming::toJoinKind(kind);
    streaming_strictness = Streaming::toJoinStrictness(strictness, table_join->isRangeJoin());
    initBufferedData();

    /// For global join versioned-kv, always keep the last version
    any_take_last_row = (streaming_strictness == Strictness::Any)
        || (right_stream_desc.data_stream_semantic == DataStreamSemantic::VersionedKV && streaming_strictness == Strictness::All)
        || emitChangeLog();

    if (any_take_last_row)
        /// Rewrite strictness to choose `HashJoin::MapsOne` hash table : left, inner + any => MapsOne
        streaming_strictness = Strictness::Any;

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(
            key_names_right, right_stream_desc.sample_block, right_data.table_keys, right_data.sample_block_with_columns_to_add);
        right_data.required_keys = table_join->getRequiredRightKeys(right_data.table_keys, right_data.required_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        /// SELECT * FROM left JOIN right ON left.key = right.key OR left.value = right.value
        right_data.sample_block_with_columns_to_add = right_data.table_keys = materializeBlock(right_stream_desc.sample_block);
    }

    initRightBlockStructure();

    chooseHashMethod();

    initHashMaps(right_data.buffered_data->getCurrentMapsVariants().map_variants);

    LOG_INFO(
        log,
        "({}) hash type: {}, kind: {}, strictness: {}, keys : {}, right header: {}",
        fmt::ptr(this),
        toString(hash_method_type),
        toString(kind),
        toString(strictness),
        TableJoin::formatClauses(table_join->getClauses(), true),
        right_stream_desc.sample_block.dumpStructure());

    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::RangeAsof)
        LOG_INFO(
            log,
            "Range join: {} join_start_bucket={} join_stop_bucket={}",
            table_join->rangeAsofJoinContext().string(),
            left_data.buffered_data->join_start_bucket,
            left_data.buffered_data->join_stop_bucket);
}

HashJoin::~HashJoin() noexcept
{
    LOG_INFO(
        log,
        "Left stream metrics: {}, right stream metrics: {}, join metrics: {}, retract buffer metrics: {}",
        left_data.buffered_data->getJoinMetrics().string(),
        right_data.buffered_data->getJoinMetrics().string(),
        join_metrics.string(),
        join_results ? join_results->metrics.string() : "");
}

/// Left stream header is only known at late stage after HashJoin is created
void HashJoin::initLeftStream(const Block & left_header)
{
    if (left_stream_desc.sample_block)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Left stream in join is already initialized");

    left_stream_desc.sample_block = left_header;

    /// If it is not bidirectional hash join, we don't care left header
    if (emitChangeLog())
    {
        /// left stream sample block's column may be not inited yet
        JoinCommon::createMissedColumns(left_stream_desc.sample_block);
        if (table_join->oneDisjunct())
        {
            const auto & key_names_left = table_join->getOnlyClause().key_names_left;
            JoinCommon::splitAdditionalColumns(
                key_names_left, left_stream_desc.sample_block, left_data.table_keys, left_data.sample_block_with_columns_to_add);
            left_data.required_keys = table_join->getRequiredLeftKeys(left_data.table_keys, left_data.required_keys_sources);
        }
        else
        {
            left_data.sample_block_with_columns_to_add = left_data.table_keys = materializeBlock(left_stream_desc.sample_block);
        }

        initLeftBlockStructure();

        initHashMaps(left_data.buffered_data->getCurrentMapsVariants().map_variants);

        join_results.emplace();
        initHashMaps(join_results->maps->map_variants);
    }
}

void HashJoin::chooseHashMethod()
{
    size_t disjuncts_num = table_join->getClauses().size();
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_data.table_keys, key_names_right);

        if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::RangeAsof
            || streaming_strictness == Strictness::Asof)
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
            asof_type = RangeAsofRowRefs::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            hash_method_type = chooseMethod(key_columns, asof_key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            hash_method_type = chooseMethod(key_columns, key_sizes.emplace_back());
        }
    }
}

HashJoin::Type HashJoin::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();
    assert(keys_size > 0);

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (isColumnConst(*key_columns[0])
                && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

void HashJoin::dataMapInit(MapsVariant & map)
{
    joinDispatchInit(streaming_kind, streaming_strictness, map);
    joinDispatch(streaming_kind, streaming_strictness, map, [&](auto, auto, auto & map_) { map_.create(hash_method_type); });
}

bool HashJoin::empty() const
{
    return hash_method_type == Type::EMPTY;
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
    assert(emitChangeLog());

    JoinCommon::convertToFullColumnsInplace(left_data.table_keys);

    initBlockStructure(left_data.buffered_data->sample_block, left_data.table_keys, left_data.sample_block_with_columns_to_add);

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
}

void HashJoin::initRightBlockStructure()
{
    JoinCommon::convertToFullColumnsInplace(right_data.table_keys);

    initBlockStructure(right_data.buffered_data->sample_block, right_data.table_keys, right_data.sample_block_with_columns_to_add);

    JoinCommon::createMissedColumns(right_data.sample_block_with_columns_to_add);
}

void HashJoin::initBlockStructure(
    Block & saved_block_sample, const Block & table_keys, const Block & sample_block_with_columns_to_add) const
{
    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns
        = !table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) || isRightOrFull(kind) || multiple_disjuncts;
    if (save_key_columns)
        saved_block_sample = table_keys.cloneEmpty();
    else if (
        streaming_strictness == Strictness::Range || streaming_strictness == Strictness::RangeAsof
        || streaming_strictness == Strictness::Asof)
        /// Save ASOF key
        saved_block_sample.insert(table_keys.safeGetByPosition(table_keys.columns() - 1));

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
    {
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }
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

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareBlockToSave(block, savedRightBlockSample());
}

Block HashJoin::prepareLeftBlock(const Block & block) const
{
    return prepareBlockToSave(block, savedLeftBlockSample());
}

bool HashJoin::addJoinedBlock(const Block & block, bool /*check_limits*/)
{
    addRightHashBlock(block); /// Copy the block
    return true;
}

inline void HashJoin::addRightHashBlock(Block source_block)
{
    /// FIXME, there are quite some block copies
    auto do_add_block = [this /*, check_limits*/](Int64 bucket, Block block) {
        /// FIXME, all_key_columns shall hold shared_ptr to columns instead of raw ptr
        /// then we can update `block` in place
        ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(block, table_join->getAllNames(JoinTableSide::Right));

        Block block_to_save = prepareRightBlock(block);

        /// FIXME, multiple disjuncts OR clause
        const auto & on_expr = table_join->getClauses().front();

        ColumnRawPtrs key_columns;
        for (const auto & name : on_expr.key_names_right)
            key_columns.push_back(all_key_columns[name]);

        /// We will insert to the map only keys, where all components are not NULL.
        ConstNullMapPtr null_map{};
        ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

        /// If RIGHT or FULL save blocks with nulls for NotJoinedBlocks
        UInt8 save_nullmap = 0;
        if (isRightOrFull(kind) && null_map)
        {
            /// Save rows with NULL keys
            for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
                save_nullmap |= (*null_map)[i];
        }

        /// Add `block_to_save` to right stream data, the following code block ideally shall be
        /// a member function of RightStreamData. But due to the coupled MapsVariant
        /// we do everything here for convenience and perf
        {
            /// Note `block_to_save` may be empty for cases in which the query doesn't care other non-key columns.
            /// For example, SELECT count() FROM stream_a JOIN stream_b ON i=ii;
            auto rows = key_columns[0]->size();

            if (bucket > 0 && bucket + right_data.buffered_data->bucket_size < combined_watermark)
            {
                LOG_INFO(
                    log,
                    "Discard {} late events in time bucket {} of right stream since it is later than latest combined watermark {} "
                    "with "
                    "bucket_size={}",
                    rows,
                    bucket,
                    combined_watermark.load(),
                    right_data.buffered_data->bucket_size);
                return;
            }

            std::scoped_lock lock(right_data.buffered_data->mutex);

            if (bucket > 0)
            {
                auto & time_bucket_hash_blocks = right_data.buffered_data->getTimeBucketHashBlocks();

                auto iter = time_bucket_hash_blocks.find(bucket);
                if (iter == time_bucket_hash_blocks.end())
                {
                    /// init this RightTableBlocks of this bucket
                    std::tie(iter, std::ignore)
                        = time_bucket_hash_blocks.emplace(bucket, right_data.buffered_data->newHashBlocks());
                    initHashMaps(iter->second->maps->map_variants);
                }

                /// Update watermark
                if (bucket > right_data.buffered_data->current_watermark)
                    right_data.buffered_data->current_watermark = bucket;

                /// Let `current_hash_blocks` points to the new HashBlocks
                right_data.buffered_data->resetCurrentHashBlocks(iter->second);
            }

            right_data.buffered_data->addBlockWithoutLock(std::move(block_to_save));
            auto & data = right_data.buffered_data->getCurrentHashBlocks();

            joinDispatch(
                streaming_kind, streaming_strictness, data.maps->map_variants[0], [&](auto /*kind_*/, auto strictness_, auto & map) {
                    [[maybe_unused]] size_t size = insertFromBlockImpl<strictness_>(
                        *this,
                        hash_method_type,
                        map,
                        rows,
                        key_columns,
                        key_sizes[0],
                        &data.blocks,
                        null_map,
                        data.pool);
                });

            if (save_nullmap)
                /// FIXME, we will need account the allocated bytes for null_map_holder / not_joined_map as well
                data.blocks_nullmaps.emplace_back(data.lastBlock(), null_map_holder);
        }
    };

    if (streaming_strictness == Strictness::All || streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Any)
    {
        do_add_block(-1, source_block);
    }
    else
    {
        auto bucketed_blocks = right_data.buffered_data->range_splitter->split(source_block);
        for (auto & bucket_block : bucketed_blocks)
            do_add_block(bucket_block.first, std::move(bucket_block.second));
    }
}

inline void HashJoin::addLeftHashBlock(Block source_block)
{
    /// FIXME, all_key_columns shall hold shared_ptr to columns instead of raw ptr
    /// then we can update `block` in place
    ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(source_block, table_join->getAllNames(JoinTableSide::Left));

    Block block_to_save = prepareLeftBlock(source_block);

    /// FIXME, multiple disjuncts OR clause
    const auto & on_expr = table_join->getClauses().front();

    ColumnRawPtrs key_columns;
    for (const auto & name : on_expr.key_names_left)
        key_columns.push_back(all_key_columns[name]);

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    /// If LEFT or FULL save blocks with nulls for NotJoinedBlocks
    UInt8 save_nullmap = 0;
    if (isLeftOrFull(kind) && null_map)
    {
        /// Save rows with NULL keys
        for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
            save_nullmap |= (*null_map)[i];
    }

    /// Add `block_to_save` to left stream data
    {
        /// Note `block_to_save` may be empty for cases in which the query doesn't care other non-key columns.
        /// For example, SELECT count() FROM stream_a JOIN stream_b ON i=ii;
        auto rows = key_columns[0]->size();

        std::scoped_lock lock(left_data.buffered_data->mutex);

        left_data.buffered_data->addBlockWithoutLock(std::move(block_to_save));
        auto & data = left_data.buffered_data->getCurrentHashBlocks();

        joinDispatch(streaming_kind, streaming_strictness, data.maps->map_variants[0], [&](auto /*kind_*/, auto strictness_, auto & map) {
            [[maybe_unused]] size_t size = insertFromBlockImpl<strictness_>(
                *this, hash_method_type, map, rows, key_columns, key_sizes[0], &data.blocks, null_map, data.pool);
        });

        if (save_nullmap)
            /// FIXME, we will need account the allocated bytes for null_map_holder / not_joined_map as well
            data.blocks_nullmaps.emplace_back(data.lastBlock(), null_map_holder);
    }
}

template <Kind KIND, Strictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    const auto & onexprs = table_join->getClauses();
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(onexprs.size());
    for (size_t i = 0, disjuncts = onexprs.size(); i < disjuncts; ++i)
    {
        const auto & key_names = onexprs[i].key_names_left;
        /// FIXME, `conColumnNames` are calculate for each join, cache it
        join_on_keys.emplace_back(block, key_names, onexprs[i].condColumnNames().first, key_sizes[i]);
    }
    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" stream must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    /// if constexpr (jf.right || jf.full)
    /// {
    ///    materializeBlockInplace(block);
    /// }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns added_columns(
        block_with_columns_to_add,
        block,
        savedRightBlockSample(),
        *this,
        std::move(join_on_keys),
        &left_data.buffered_data->getCurrentJoinedMap(),
        left_data.buffered_data->range_asof_join_ctx,
        jf.is_asof_join || jf.is_range_asof_join || jf.is_range_join);

    bool has_required_right_keys = (right_data.required_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_right_keys;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, hash_method_type);

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0; i < right_data.required_keys.columns(); ++i)
        {
            const auto & right_key = right_data.required_keys.getByPosition(i);
            // renamed ???
            if (!block.findByName(right_key.name))
            {
                const auto & left_name = right_data.required_keys_sources[i];

                /// asof column is already in block.
                if ((jf.is_asof_join || jf.is_range_asof_join) && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = JoinCommon::isNullable(right_key.type);
                auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
                ColumnWithTypeAndName right_col(col.column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = JoinCommon::correctNullability(std::move(right_col), is_nullable);
                block.insert(right_col);
            }
        }
    }
    else if (has_required_right_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < right_data.required_keys.columns(); ++i)
        {
            const auto & right_key = right_data.required_keys.getByPosition(i);
            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            if (!block.findByName(right_col_name /*right_key.name*/))
            {
                const auto & left_name = right_data.required_keys_sources[i];

                /// asof column is already in block.
                if ((jf.is_asof_join || jf.is_range_asof_join) && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = JoinCommon::isNullable(right_key.type);

                ColumnPtr thin_column = JoinCommon::filterWithBlanks(col.column, filter);

                ColumnWithTypeAndName right_col(thin_column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = JoinCommon::correctNullability(std::move(right_col), is_nullable, null_map_filter);
                block.insert(right_col);

                if constexpr (jf.need_replication)
                    right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
            }
        }
    }

    if constexpr (jf.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }
}

/// Join with left hash table with right block
template <Kind KIND, Strictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImplLeft(Block & right_block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & on_exprs = table_join->getClauses();
    for (size_t i = 0, disjuncts = on_exprs.size(); i < disjuncts; ++i)
    {
        const auto & key_names = on_exprs[i].key_names_right;
        join_on_keys.emplace_back(right_block, key_names, on_exprs[i].condColumnNames().second, key_sizes[i]);
    }
    size_t existing_columns = right_block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" stream must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    ///if constexpr (jf.right || jf.full)
    ///{
    ///    materializeBlockInplace(block);
    ///}

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      * but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns added_columns(
        block_with_columns_to_add,
        right_block,
        savedLeftBlockSample(),
        *this,
        std::move(join_on_keys),
        &right_data.buffered_data->getCurrentJoinedMap(),
        right_data.buffered_data->range_asof_join_ctx,
        /*is_asof_join=*/ false, /// We don't support asof join for bidirectional hash join
        /*is_left_block=*/ false);

    bool has_required_left_keys = (left_data.required_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_left_keys;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, hash_method_type);

    for (size_t i = 0, add_col_size = added_columns.size(); i < add_col_size; ++i)
        right_block.insert(added_columns.moveColumn(i));

    std::vector<size_t> left_keys_to_replicate [[maybe_unused]];

    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            right_block.safeGetByPosition(i).column = right_block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0, required_keys_size = left_data.required_keys.columns(); i < required_keys_size; ++i)
        {
            const auto & left_key = left_data.required_keys.getByPosition(i);
            if (!right_block.findByName(left_key.name))
            {
                const auto & right_name = left_data.required_keys_sources[i];

                const auto & col = right_block.getByName(right_name);
                bool is_nullable = JoinCommon::isNullable(left_key.type);
                ColumnWithTypeAndName left_col(col.column, col.type, left_key.name);
                if (left_col.type->lowCardinality() != left_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(left_col);
                left_col = JoinCommon::correctNullability(std::move(left_col), is_nullable);
                right_block.insert(left_col);
            }
        }
    }
    else if (has_required_left_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add join key columns from left block if needed.
        for (size_t i = 0; i < left_data.required_keys.columns(); ++i)
        {
            const auto & left_key = left_data.required_keys.getByPosition(i);
            if (!right_block.findByName(left_key.name))
            {
                const auto & right_name = left_data.required_keys_sources[i];
                const auto & col = right_block.getByName(right_name);
                bool is_nullable = JoinCommon::isNullable(left_key.type);

                ColumnPtr thin_column = JoinCommon::filterWithBlanks(col.column, filter);

                ColumnWithTypeAndName left_col(thin_column, col.type, left_key.name);
                if (left_col.type->lowCardinality() != left_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(left_col);
                left_col = JoinCommon::correctNullability(std::move(left_col), is_nullable, null_map_filter);
                right_block.insert(left_col);

                if constexpr (jf.need_replication)
                    left_keys_to_replicate.push_back(right_block.getPositionByName(left_key.name));
            }
        }
    }

    if constexpr (jf.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            right_block.safeGetByPosition(i).column = right_block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : left_keys_to_replicate)
            right_block.safeGetByPosition(pos).column = right_block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_data.table_keys, onexpr.key_names_right);
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & /*not_processed*/)
{
    doJoinBlock(block);
}

inline void HashJoin::doJoinBlock(Block & block)
{
    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block,
            onexpr.key_names_left,
            cond_column_name.first,
            right_stream_desc.sample_block,
            onexpr.key_names_right,
            cond_column_name.second);
    }

    auto & data = right_data.buffered_data->getCurrentHashBlocks();
    std::vector<const std::decay_t<decltype(data.maps->map_variants[0])> *> maps_vector;
    for (size_t i = 0; i < table_join->getClauses().size(); ++i)
        maps_vector.push_back(&data.maps->map_variants[i]);

    if (joinDispatch(streaming_kind, streaming_strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_) {
            joinBlockImpl<kind_, strictness_>(block, right_data.sample_block_with_columns_to_add, maps_vector_);
        }))
    {
        /// Joined
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
}

/// Use left_block to join right hash table
Block HashJoin::joinWithRightBlocks(Block & left_block)
{
    assert(emitChangeLog());
    assert(streaming_strictness == Strictness::Any);

    doJoinBlock(left_block);

    auto rows = left_block.rows();
    if (rows)
    {
        if (!left_block.has(ProtonConsts::RESERVED_DELTA_FLAG))
            addDeltaColumn(left_block, rows);

        return retract(left_block);
    }

    return {};
}

/// Use right_block to join left hash table
Block HashJoin::joinWithLeftBlocks(Block & right_block, const Block & output_header)
{
    assert(emitChangeLog());
    assert(streaming_strictness == Strictness::Any);

    for (const auto & on_expr : table_join->getClauses())
    {
        auto cond_column_name = on_expr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            right_block,
            on_expr.key_names_right,
            cond_column_name.second,
            left_stream_desc.sample_block,
            on_expr.key_names_left,
            cond_column_name.first);
    }

    auto & data = left_data.buffered_data->getCurrentHashBlocks();

    std::vector<const std::decay_t<decltype(data.maps->map_variants[0])> *> maps_vector;
    for (size_t i = 0, disjuncts = table_join->getClauses().size(); i < disjuncts; ++i)
        maps_vector.push_back(&data.maps->map_variants[i]);

    auto flipped_kind = flipKind(streaming_kind);
    if (joinDispatch(flipped_kind, streaming_strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_) {
            joinBlockImplLeft<kind_, strictness_>(right_block, left_data.sample_block_with_columns_to_add, maps_vector_);
        }))
    {
        /// Joined
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);


    auto rows = right_block.rows();
    if (rows)
    {
        if (!right_block.has(ProtonConsts::RESERVED_DELTA_FLAG))
            addDeltaColumn(right_block, rows);

        /// Re-arrange columns according to output header
        right_block.reorderColumnsInBlock(output_header);

        /// assertBlocksHaveEqualStructure(right_block, output_header, "Join Left");
        /// assert(blocksHaveEqualStructure(right_block, output_header));

        return retract(right_block);
    }
    return {};
}

template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        constexpr bool mapped_range_asof = std::is_same_v<Mapped, RangeAsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

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

std::shared_ptr<NotJoinedBlocks>
HashJoin::getNonJoinedBlocks(const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    /// FIXME, if there are left rows which are not joined with right data
    return {};
}

const ColumnWithTypeAndName & HashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable if nullable_right_side is true
    return savedRightBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::initBufferedData()
{
    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::RangeAsof)
    {
        const auto & right_asof_key_col = table_join->getOnlyClause().key_names_right.back();
        const auto & left_asof_key_col = table_join->getOnlyClause().key_names_left.back();

        const auto & range_join_ctx = table_join->rangeAsofJoinContext();

        right_data.buffered_data = std::make_unique<BufferedStreamData>(this, range_join_ctx, right_asof_key_col);
        left_data.buffered_data = std::make_unique<BufferedStreamData>(this, range_join_ctx, left_asof_key_col);
    }
    else
    {
        /// Although joining `versioned_kv` doesn't need hold left data in memory
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

Block HashJoin::joinBlocksAll(size_t & left_cached_bytes, size_t & right_cached_bytes)
{
    Block result;

    std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

    left_cached_bytes = left_data.buffered_data->getJoinMetrics().total_bytes;
    right_cached_bytes = right_data.buffered_data->getJoinMetrics().total_bytes;

    ++join_metrics.total_join;

    if (!left_data.buffered_data->hasNewData() && !right_data.buffered_data->hasNewData())
    {
        ++join_metrics.no_new_data_skip;
        return result;
    }

    auto & left_current_blocks = left_data.buffered_data->getCurrentHashBlocks();
    auto & right_current_hash_blocks = right_data.buffered_data->getCurrentHashBlocks();

    if (right_current_hash_blocks.blocks.empty() && streaming_kind != Kind::Left)
        return result;

    auto left_block_start = left_current_blocks.blocks.begin();
    if (!right_data.buffered_data->hasNewData())
    {
        /// If left stream has new data and right stream doesn't have new data
        /// Only join new data from left bucket with right bucket data
        left_block_start = left_current_blocks.new_data_iter;
        ++join_metrics.only_join_new_data;
    }

    auto left_block_end = left_current_blocks.blocks.end();
    for (; left_block_start != left_block_end; ++left_block_start)
    {
        auto join_block{left_block_start->block}; /// need a copy here since joinBlock will change the block passed-in in place

        doJoinBlock(join_block);

        if (join_block.rows())
        {
            /// Has joined rows
            if (result)
            {
                /// assertBlocksHaveEqualStructure(result, join_block, "Hashed joined block");
                assert(blocksHaveEqualStructure(result, join_block));
                for (size_t i = 0; auto & source_column : join_block)
                {
                    auto mutable_column = IColumn::mutate(std::move(result.getByPosition(i).column));
                    mutable_column->insertRangeFrom(*source_column.column, 0, source_column.column->size());
                    result.getByPosition(i).column = std::move(mutable_column);
                    ++i;
                }
            }
            else
            {
                result.swap(join_block);
            }
        }
    }

    /// After full scan join, clear the new data flag. An optimization for periodical timeout which triggers join
    /// and there is no new data inserted
    left_data.buffered_data->setHasNewData(false);
    right_data.buffered_data->setHasNewData(false);

    return result;
}

Block HashJoin::joinBlocksBidirectional(size_t & left_cached_bytes, size_t & right_cached_bytes)
{
    Block result;

    std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

    left_cached_bytes = left_data.buffered_data->getJoinMetrics().total_bytes;
    right_cached_bytes = right_data.buffered_data->getJoinMetrics().total_bytes;

    ++join_metrics.total_join;

    if (!left_data.buffered_data->hasNewData() && !right_data.buffered_data->hasNewData())
    {
        ++join_metrics.no_new_data_skip;
        return result;
    }

    /// FIXME, not finished yet
    return result;
}

Block HashJoin::joinBlocks(size_t & left_cached_bytes, size_t & right_cached_bytes)
{
    if (emitChangeLog())
        return joinBlocksBidirectional(left_cached_bytes, right_cached_bytes);

    if (streaming_strictness == Strictness::All)
        return joinBlocksAll(left_cached_bytes, right_cached_bytes);

    /// Range join
    Block result;
    {
        std::scoped_lock lock(left_data.buffered_data->mutex, right_data.buffered_data->mutex);

        left_cached_bytes = left_data.buffered_data->getJoinMetrics().total_bytes;
        right_cached_bytes = right_data.buffered_data->getJoinMetrics().total_bytes;

        ++join_metrics.total_join;

        if (!left_data.buffered_data->hasNewData() && !right_data.buffered_data->hasNewData())
        {
            ++join_metrics.no_new_data_skip;
            return result;
        }

        /// In this case, left part blocks are not hashed indexed
        auto & left_time_bucket_blocks = left_data.buffered_data->getTimeBucketHashBlocks();
        auto & right_time_bucket_hash_blocks = right_data.buffered_data->getTimeBucketHashBlocks();

        for (auto & [left_bucket, left_bucket_blocks] : left_time_bucket_blocks)
        {
            auto right_iter = right_time_bucket_hash_blocks.lower_bound(
                left_bucket - left_data.buffered_data->bucket_size * left_data.buffered_data->join_start_bucket);

            if (right_iter == right_time_bucket_hash_blocks.end())
                /// no joined data
                continue;

            left_data.buffered_data->resetCurrentHashBlocks(left_bucket_blocks);

            for (auto right_begin = right_iter, right_end = right_time_bucket_hash_blocks.end(); right_begin != right_end; ++right_begin)
            {
                if (right_begin->first > left_bucket + left_data.buffered_data->bucket_size * left_data.buffered_data->join_stop_bucket)
                    /// Reaching the upper bound of right bucket to join
                    break;

                auto & right_bucket_blocks = right_begin->second;
                if (!left_bucket_blocks->hasNewData() && !right_bucket_blocks->hasNewData())
                {
                    /// Ignore this bucket if there are no new data on both bucket since last join
                    ++join_metrics.time_bucket_no_new_data_skip;
                    continue;
                }

                /// Although we are bucketing blocks, but the real min/max in 2 buckets may not be join-able
                /// If [min, max] of left bucket doesn't intersect with right time bucket as a whole,
                /// we are sure there will be no join-able rows for the whole bucket
                if (!left_data.buffered_data->intersect(
                        left_bucket_blocks->blocks.minTimestamp(),
                        left_bucket_blocks->blocks.maxTimestamp(),
                        right_bucket_blocks->blocks.minTimestamp(),
                        right_bucket_blocks->blocks.maxTimestamp()))
                {
                    ++join_metrics.time_bucket_no_intersection_skip;
                    continue;
                }

                /// Setup right current_hash_blocks which will be used by `joinBlock`
                right_data.buffered_data->resetCurrentHashBlocks(right_bucket_blocks);

                auto left_block_start = left_bucket_blocks->blocks.begin();
                if (left_bucket_blocks->hasNewData() && !right_bucket_blocks->hasNewData())
                {
                    /// If left bucket has new data and right bucket doesn't have new data
                    /// Only join new data from left bucket with right bucket data
                    left_block_start = left_bucket_blocks->new_data_iter;
                    ++join_metrics.only_join_new_data;
                }

                for (auto left_block_end = left_bucket_blocks->blocks.end(); left_block_start != left_block_end; ++left_block_start)
                {
                    /// If [min, max] of left block doesn't intersect with right time bucket
                    /// we are sure there will be no join-able rows
                    if (!left_data.buffered_data->intersect(
                            left_block_start->block.info.watermark_lower_bound,
                            left_block_start->block.info.watermark,
                            right_bucket_blocks->blocks.minTimestamp(),
                            right_bucket_blocks->blocks.maxTimestamp()))
                    {
                        ++join_metrics.left_block_and_right_time_bucket_no_intersection_skip;
                        continue;
                    }

                    /// auto join_block{left_block_start->deepClone()};
                    auto join_block{left_block_start->block}; /// need a copy here since joinBlock will change the block passed-in in place

                    doJoinBlock(join_block);

                    if (join_block.rows())
                    {
                        /// Has joined rows
                        if (result)
                        {
                            assert(blocksHaveEqualStructure(result, join_block));
                            /// assertBlocksHaveEqualStructure(result, join_block, "Hashed joined block");
                            for (size_t i = 0; auto & source_column : join_block)
                            {
                                auto mutable_column = IColumn::mutate(std::move(result.getByPosition(i).column));
                                mutable_column->insertRangeFrom(*source_column.column, 0, source_column.column->size());
                                result.getByPosition(i).column = std::move(mutable_column);
                                ++i;
                            }
                        }
                        else
                        {
                            result.swap(join_block);
                        }
                    }
                }

                /// After join, we consumed all new data. Mark the status. It is an optimization
                right_bucket_blocks->markNoNewData();
                left_bucket_blocks->markNoNewData();
            }
        }

        /// After full scan join, clear the new data flag. An optimization for periodical timeout which triggers join
        /// and there is no new data inserted
        left_data.buffered_data->setHasNewData(false);
        right_data.buffered_data->setHasNewData(false);
    }

    calculateWatermark();

    left_cached_bytes = left_data.buffered_data->removeOldBuckets("left_stream");
    right_cached_bytes = right_data.buffered_data->removeOldBuckets("right_stream");

    return result;
}

size_t HashJoin::insertLeftBlock(Block left_input_block)
{
    if (emitChangeLog())
    {
        addLeftHashBlock(std::move(left_input_block));
    }
    else
    {
        if (streaming_strictness == Strictness::All || streaming_strictness == Strictness::Asof)
            left_data.buffered_data->addBlock(std::move(left_input_block));
        else
            left_data.buffered_data->addBlockToTimeBucket(std::move(left_input_block));
    }

    /// We don't hold the lock, it is ok to have stale numbers
    return left_data.buffered_data->getJoinMetrics().total_bytes;
}

size_t HashJoin::insertRightBlock(Block right_input_block)
{
    addRightHashBlock(std::move(right_input_block));
    /// We don't hold the lock, it is ok to have stale numbers
    return right_data.buffered_data->getJoinMetrics().total_bytes;
}

void HashJoin::calculateWatermark()
{
    Int64 left_watermark = left_data.buffered_data->current_watermark;
    Int64 right_watermark = right_data.buffered_data->current_watermark;

    if (left_watermark == 0 || right_watermark == 0)
        return;

    Int64 last_combined_watermark = combined_watermark;
    auto new_combined_watermark = std::min(left_watermark, right_watermark);

    if (last_combined_watermark != new_combined_watermark)
    {
        combined_watermark = new_combined_watermark;

        LOG_INFO(
            log,
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
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRefWithRefCount>;

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

                    mapped.block_iter->block.insertRow(mapped.row_num, retracted_block);

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
    join_results->blocks.push_back(result_block);

    const auto & on_exprs = table_join->getClauses();
    auto disjuncts = on_exprs.size();

    /// Second, Build hash index for join results and in the meanwhile build retract block
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(on_exprs.size());
    for (size_t i = 0; i < disjuncts; ++i)
    {
        /// FIXME, can we assume left keys are always in result_block ?
        const auto & key_names = on_exprs[i].key_names_left;
        /// FIXME, `conColumnNames` are calculate for each join, cache it
        join_on_keys.emplace_back(result_block, key_names, on_exprs[i].condColumnNames().first, key_sizes[i]);
    }

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
        case HashJoin::Type::TYPE: { \
            using MapTypeVal = typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
            using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, MapTypeVal>::Type; \
            std::vector<MapTypeVal *> a_map_type_vector(mapv.size()); \
            std::vector<KeyGetter> key_getter_vector; \
            for (size_t d = 0; d < disjuncts; ++d) \
            { \
                const auto & join_on_key = join_on_keys[d]; \
                a_map_type_vector[d] = mapv[d]->TYPE.get(); \
                key_getter_vector.push_back(std::move(createKeyGetter<KeyGetter, false>(join_on_key.key_columns, join_on_key.key_sizes))); \
            }  \
            doRetract(result_block, std::move(key_getter_vector), a_map_type_vector, *join_results, retracted_block); \
            break; \
        }
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

            default:
                UNREACHABLE();
        }
    });

    if (retracted_block.rows())
    {
        /// Update _tp_delta from 1 => -1
        auto & col = retracted_block.getByName(ProtonConsts::RESERVED_DELTA_FLAG);
        auto & data = assert_cast<ColumnInt8 &>(col.column->assumeMutableRef()).getData();
        std::fill(data.begin(), data.end(), -1);

        /// col.column = col.type->createColumnConst(retracted_block.rows(), -1);
    }
    return retracted_block;
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
        const auto & cond_column_names = on_expr.condColumnNames();
        if (!cond_column_names.first.empty() || !cond_column_names.second.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Streaming join doesn't support predicates in JOIN ON clause. Use WHERE predicate instead");
    }

    auto throw_ex = [this]() {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "'{}' {} {} join '{}' is not supported",
            magic_enum::enum_name(left_stream_desc.data_stream_semantic),
            toString(kind),
            toString(strictness),
            magic_enum::enum_name(right_stream_desc.data_stream_semantic));
    };

    /// [INNER] JOIN : only matching rows are returned
    /// LEFT [OUTER] JOIN: non-matching rows from left stream are returned in addition to matching rows
    /// RIGHT [OUTER] JOIN: non-matching rows from right stream are returned in addition to matching rows
    /// FULL [OUTER] JOIN: non-matching rows from both streams are returned in addition to matching rows
    /// CROSS JOIN: produces a cartesian product of the 2 streams. "join keys" are not specified
    ///
    /// Non-standard join types:
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

    /// So far we only support the following join combinations

    /// Check the join strictness
    bool valid_join = strictness == JoinStrictness::All || strictness == JoinStrictness::Asof;
    if (!valid_join)
        throw_ex();

    /// 1) append-only [inner | left] join [right_stream]
    valid_join = left_stream_desc.data_stream_semantic == DataStreamSemantic::Append;
    if (valid_join)
    {
        /// Check the join type
        valid_join = isInner(kind) || isLeft(kind);
        if (!valid_join)
            throw_ex();
        return;
    }

    /// 2) versioned-kv [inner | left] join versioned-kv
    valid_join = left_stream_desc.data_stream_semantic == DataStreamSemantic::VersionedKV
        && right_stream_desc.data_stream_semantic == DataStreamSemantic::VersionedKV;
    if (valid_join)
    {
        /// Check the join type
        valid_join = isInner(kind) || isLeft(kind) || isFull(kind) || isFull(kind);
        if (!valid_join)
            throw_ex();
        return;
    }

    /// 3) changelog-kv join changelog-kv
    valid_join = left_stream_desc.data_stream_semantic == DataStreamSemantic::ChangeLogKV
        && right_stream_desc.data_stream_semantic == DataStreamSemantic::ChangeLogKV;
    if (valid_join)
    {
        /// Check the join type
        valid_join = isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind);
        if (!valid_join)
            throw_ex();
        return;
    }

    throw_ex();
}

}
}
