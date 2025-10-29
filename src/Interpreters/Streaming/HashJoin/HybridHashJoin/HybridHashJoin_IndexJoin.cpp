#include <Interpreters/Streaming/HashJoin/AddedColumns.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>
#include <Interpreters/Streaming/HashJoin/RowUtil.h>
#include <Interpreters/Streaming/HashJoin/joinCommon.h>

#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int INTERNAL_ERROR;
}

namespace Streaming
{

namespace
{

template <typename KeyGetter, bool is_asof_join>
KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const std::vector<size_t> & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_columns_v = std::span(key_columns.begin(), key_columns.size() - 1);
        auto key_sizes_v = std::span(key_sizes.begin(), key_sizes.size() - 1);
        return KeyGetter(key_columns_v, key_sizes_v);
    }
    else
    {
        return KeyGetter(key_columns, key_sizes);
    }
}

/// \return true if compared equal otherwise false
bool ALWAYS_INLINE compareEqual(const Block & block, size_t row, const Row & one_row, const std::vector<size_t> & skip_columns)
{
    chassert(block.columns() == one_row.size());

    for (size_t i = 0, num_columns = one_row.size(); i < num_columns; ++i)
    {
        if (std::ranges::find(skip_columns, i) != skip_columns.end())
            continue;

        const auto & lhs_col = block.getByPosition(i);

        if (!lhs_col.column->equal(row, one_row[i], -1))
            return false;
    }

    return true;
}

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

template <bool add_missing>
void addFoundRowAll(const AllRows & all_rows, AddedColumns & added_columns, IColumn::Offset & current_offset)
{
    if constexpr (add_missing)
        added_columns.applyLazyDefaults();

    for (const auto & batch : all_rows.batches)
    {
        auto rows = batch.rows();
        for (size_t row = 0; row < rows; ++row)
        {
            added_columns.appendFromBlock<false>(batch, row);
            ++current_offset;
        }
    }
};

/// Joins columns which indexes are present in the corresponding index using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <
    Kind KIND,
    Strictness STRICTNESS,
    typename KeyGetter,
    bool has_null_map,
    bool need_filter,
    bool /*multiple_disjuncts*/,
    typename Map>
NO_INLINE IColumn::Filter joinColumns(std::vector<std::vector<Map *>> & map_vv, AddedColumns & added_columns)
{
    constexpr JoinFeatures<KIND, STRICTNESS, HybridMapGetter> jf;
    constexpr bool is_asof_join = (STRICTNESS == Strictness::Asof || STRICTNESS == Strictness::Range);

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    if constexpr (jf.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;

    std::vector<KeyGetter> key_getters;
    key_getters.reserve(added_columns.join_on_keys.size());
    for (size_t onexpr_idx = 0, join_exprs = added_columns.join_on_keys.size(); onexpr_idx < join_exprs; ++onexpr_idx)
    {
        const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
        key_getters.emplace_back(createKeyGetter<KeyGetter, is_asof_join>(join_keys.key_columns, join_keys.key_sizes));
    }

    for (size_t i = 0; i < rows; ++i)
    {
        bool right_row_found = false;
        bool null_element_found = false;

        for (size_t onexpr_idx = 0, join_exprs = added_columns.join_on_keys.size(); onexpr_idx < join_exprs; ++onexpr_idx)
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

            auto & key_getter = key_getters[onexpr_idx];
            auto key = key_getter.getKeyHolder(i);

            bool row_acceptable = !join_keys.isRowFiltered(i);

            auto & map_v = map_vv[onexpr_idx];
            if constexpr (jf.is_range_join)
                chassert(map_v.size() >= 1);
            else
                chassert(map_v.size() == 1);

            for (auto * map : map_v)
            {
                auto find_result = row_acceptable ? map->findKey(key, /*disable_spill=*/true) : HybridFindResult{};
                if (find_result.hasError())
                    throw Exception::createRuntime(find_result.errcode, find_result.errorString());

                if (find_result.isFound())
                {
                    if constexpr (jf.is_range_join)
                    {
                        TypeIndex asof_type = added_columns.asofType();
                        const IColumn & asof_key = added_columns.asofKey();

                        const auto * mapped = static_cast<const RangeAsofRows *>(find_result.getMapped());
                        if (auto row_refs
                            = mapped->findRange(asof_type, added_columns.range_join_ctx, asof_key, i, added_columns.is_left_block);
                            !row_refs.empty())
                        {
                            right_row_found = true;
                            setUsed<need_filter>(filter, i);
                            for (auto & row_ref : row_refs)
                            {
                                chassert(row_ref.iter);
                                added_columns.appendFromRow<jf.add_missing>(**row_ref.iter);
                                ++current_offset;
                            }
                        }
                    }
                    else if constexpr (jf.is_asof_join)
                    {
                        TypeIndex asof_type = added_columns.asofType();
                        ASOFJoinInequality asof_inequality = added_columns.asofInequality();
                        const IColumn & asof_key = added_columns.asofKey();

                        const auto * mapped = static_cast<const AsofRows *>(find_result.getMapped());
                        if (const auto * found = mapped->findAsof(asof_type, asof_inequality, asof_key, i))
                        {
                            right_row_found = true;
                            setUsed<need_filter>(filter, i);
                            added_columns.appendFromRow<jf.add_missing>(**found->iter);
                        }
                    }
                    else if constexpr (jf.is_all_join)
                    {
                        right_row_found = true;
                        const auto * mapped = static_cast<const AllRows *>(find_result.getMapped());
                        setUsed<need_filter>(filter, i);
                        addFoundRowAll<jf.add_missing>(*mapped, added_columns, current_offset);
                    }
                    else if constexpr (jf.is_multi_join)
                    {
                        const auto * mapped = static_cast<const RowList *>(find_result.getMapped());
                        if (!mapped->empty())
                        {
                            right_row_found = true;
                            setUsed<need_filter>(filter, i);
                            for (const auto & one_row : mapped->rows)
                            {
                                added_columns.appendFromRow<jf.add_missing>(one_row);
                                ++current_offset;
                            }
                        }
                    }
                    else if constexpr (jf.is_latest_join && (jf.inner || jf.left || jf.full))
                    {
                        right_row_found = true;
                        const auto * mapped = static_cast<const OneRow *>(find_result.getMapped());
                        setUsed<need_filter>(filter, i);
                        added_columns.appendFromRow<jf.add_missing>(mapped->row);
                        break;
                    }
                    else
                    {
                        /// FIXME
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "right latest join is not supported");
                    }
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

    for (auto & map_v : map_vv)
        for (auto * map : map_v)
            map->spillIfNecessary();

    return filter;
}

template <Kind KIND, Strictness STRICTNESS, bool has_null_map, typename TaggedMap>
IColumn::Filter switchJoinColumns(std::vector<std::vector<TaggedMap *>> & map_vv_, AddedColumns & added_columns, HybridHashType type)
{
    /// [[maybe_unused]] constexpr bool is_asof_join = (STRICTNESS == Strictness::Asof || STRICTNESS == Strictness::Range);
    chassert(!map_vv_.empty() && !map_vv_.front().empty());

    switch (type)
    {
        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Invalid hash type='{}' was chosen for hybrid hash join", magic_enum::enum_name(type));
        }

#define M(TYPE, IS_TWO_LEVEL) \
    case HybridHashType::TYPE: \
    { \
        using MapType = std::decay_t<typename std::remove_reference_t<decltype(map_vv_.front().front()->table.TYPE)>::element_type>; \
        using KeyGetter = HybridKeyGetter<HybridHashType::TYPE, /*has_null_map=*/false>; \
        std::vector<std::vector<MapType *>> map_vv{map_vv_.size()}; \
        for (size_t d = 0; d < map_vv_.size(); ++d) \
            for (const auto * tagged : map_vv_[d]) \
                map_vv[d].push_back(tagged->table.TYPE.get()); \
\
        if (added_columns.need_filter) \
        { \
            return map_vv.size() > 1 \
                ? joinColumns<KIND, STRICTNESS, KeyGetter, has_null_map, /*need_filter=*/true, /*multiple_disjuncts=*/true>( \
                    map_vv, added_columns) \
                : joinColumns<KIND, STRICTNESS, KeyGetter, has_null_map, /*need_filter=*/true, /*multiple_disjuncts=*/false>( \
                    map_vv, added_columns); \
        } \
        else \
        { \
            return map_vv.size() > 1 \
                ? joinColumns<KIND, STRICTNESS, KeyGetter, has_null_map, /*need_filter=*/false, /*multiple_disjuncts=*/true>( \
                    map_vv, added_columns) \
                : joinColumns<KIND, STRICTNESS, KeyGetter, has_null_map, /*need_filter=*/false, /*multiple_disjuncts=*/false>( \
                    map_vv, added_columns); \
        } \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_JOIN_HYBRID(M)
#undef M
    }
}

/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <typename Map, typename KeyGetter>
struct Inserter
{
    static ALWAYS_INLINE void insertAll(Map & map, KeyGetter & key_getter, Block & saved_block, size_t row)
    {
        auto key = key_getter.getKeyHolder(row);
        auto emplace_result = map.emplaceKey(key, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        auto * all_rows = static_cast<AllRows *>(emplace_result.getMutableMapped());
        /// The first element of the list is stored in the value of the hash table, the rest in the pool.
        all_rows->insert(saved_block, row);
    }

    static ALWAYS_INLINE void insertOne(HybridHashJoin & join, Map & map, KeyGetter & key_getter, Block & saved_block, size_t row)
    {
        auto key = key_getter.getKeyHolder(row);
        auto emplace_result = map.emplaceKey(key, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        if (emplace_result.isInserted())
        {
            auto * one_row = static_cast<OneRow *>(emplace_result.getMutableMapped());
            one_row->row.resize(saved_block.columns());
            one_row->update(saved_block, row);
        }
        else if (join.anyTakeLastRow())
        {
            auto * one_row = static_cast<OneRow *>(emplace_result.getMutableMapped());
            one_row->update(saved_block, row);
        }
    }

    static ALWAYS_INLINE void
    insertMultiple(Map & map, KeyGetter & key_getter, Block & saved_block, size_t row, IColumn::Filter * new_keys_filter)
    {
        auto key = key_getter.getKeyHolder(row);
        auto emplace_result = map.emplaceKey(key, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        auto * mapped = static_cast<RowList *>(emplace_result.getMutableMapped());
        if (emplace_result.isInserted())
        {
            if (new_keys_filter)
                (*new_keys_filter)[row] = 1;
        }

        mapped->insert(saved_block, row);
    }

    static ALWAYS_INLINE void insertAsof(
        HybridHashJoin & join,
        Map & map,
        KeyGetter & key_getter,
        Block & saved_block,
        size_t row,
        const IColumn & asof_column,
        UInt64 keep_versions)
    {
        auto key = key_getter.getKeyHolder(row);
        auto emplace_result = map.emplaceKey(key, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        TypeIndex asof_type = *join.getAsofType();

        auto * asof_rows = static_cast<AsofRows *>(emplace_result.getMutableMapped());
        asof_rows->insert(asof_type, asof_column, saved_block, row, join.getAsofInequality(), keep_versions);
    }

    static ALWAYS_INLINE void
    insertRangeAsof(HybridHashJoin & join, Map & map, KeyGetter & key_getter, Block & saved_block, size_t row, const IColumn & asof_column)
    {
        auto key = key_getter.getKeyHolder(row);
        auto emplace_result = map.emplaceKey(key, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        TypeIndex asof_type = *join.getAsofType();
        auto * range_asof_rows = static_cast<RangeAsofRows *>(emplace_result.getMutableMapped());
        range_asof_rows->insert(asof_type, asof_column, saved_block, row);
    }
};

template <Strictness STRICTNESS, bool has_null_map, typename KeyGetter, typename TaggedType, typename Map>
size_t NO_INLINE insertFromBlockImplType(
    HybridHashJoin & join,
    Map & map,
    const ColumnRawPtrs & key_columns,
    const std::vector<size_t> & key_sizes,
    Block & saved_block,
    ConstNullMapPtr null_map,
    IColumn::Filter * new_keys_filter)
{
    constexpr bool mapped_one = std::is_same_v<TaggedType, OneRow>;
    constexpr bool mapped_multiple = std::is_same_v<TaggedType, RowList>;
    constexpr bool is_range_asof_join = (STRICTNESS == Strictness::Range);
    constexpr bool is_asof_join = (STRICTNESS == Strictness::Asof);

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_range_asof_join || is_asof_join)
        asof_column = key_columns.back();

    KeyGetter key_getter = createKeyGetter < KeyGetter, is_range_asof_join || is_asof_join > (key_columns, key_sizes);
    auto rows = saved_block.rows();

    for (size_t row = 0; row < rows; ++row)
    {
        if constexpr (has_null_map)
        {
            if (null_map && (*null_map)[row])
                continue;
        }

        if constexpr (is_range_asof_join)
            Inserter<Map, KeyGetter>::insertRangeAsof(join, map, key_getter, saved_block, row, *asof_column);
        else if constexpr (is_asof_join)
            Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, saved_block, row, *asof_column, join.keepVersions());
        else if constexpr (mapped_one)
            Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, saved_block, row);
        else if constexpr (mapped_multiple)
            /// So far \new_keys_filter is just used in multiple map for bidirectional join
            Inserter<Map, KeyGetter>::insertMultiple(map, key_getter, saved_block, row, new_keys_filter);
        else
            Inserter<Map, KeyGetter>::insertAll(map, key_getter, saved_block, row);
    }

    /// In insert loop, we disable spill and we do bulk spill here
    map.spillIfNecessary();

    return map.getBufferSizeInCells();
}

template <Strictness STRICTNESS, typename TaggedMap>
size_t insertFromBlockImpl(
    HybridHashJoin & join,
    HybridHashType type,
    TaggedMap & tagged_map,
    const ColumnRawPtrs & key_columns,
    const std::vector<size_t> & key_sizes,
    Block & saved_block,
    ConstNullMapPtr null_map,
    IColumn::Filter * new_keys_filter)
{
    using TaggedType = typename TaggedMap::TaggedType;
    switch (type)
    {
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid hash type='{}' chosen for hybrid hash join", magic_enum::enum_name(type));
        }

#define M(TYPE, IS_TWO_LEVEL) \
    case HybridHashType::TYPE: \
    { \
        if (null_map) \
            return insertFromBlockImplType< \
                STRICTNESS, \
                /*has_null_map=*/true, \
                HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/false>, \
                TaggedType>(join, *tagged_map.table.TYPE, key_columns, key_sizes, saved_block, null_map, new_keys_filter); \
        else \
            return insertFromBlockImplType< \
                STRICTNESS, \
                /*has_null_map=*/false, \
                HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/false>, \
                TaggedType>(join, *tagged_map.table.TYPE, key_columns, key_sizes, saved_block, null_map, new_keys_filter); \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_JOIN_HYBRID(M)
#undef M
    }
}

}

bool HybridHashJoin::addJoinedBlock(const Block & block, [[maybe_unused]] bool check_limits)
{
    doInsertBlock<false>(block, right_data.index->getCurrentMapsVariants()); /// Copy the block
    return true;
}

/// For non-bidirectional hash join
void HybridHashJoin::insertRightBlock(Block right_block)
{
    chassert(!range_bidirectional_hash_join && !bidirectional_hash_join);

    std::scoped_lock lock(right_data.index->mutex);

    /// There is an implicit assumption that retract block is standalone
    /// a.k.a all rows in the block having _tp_delta as -1
    if (isRetractBlock(right_block, *right_data.join_ctx.join_stream_desc))
    {
        eraseExistingKeys<false>(right_block, right_data);
        return;
    }

    /// TODO... changelog transform push down optimization is not implemented yet
    /// auto row_refs = eraseOrAppendForPartialPrimaryKeyJoin(right_block);
    doInsertBlock<false>(std::move(right_block), right_data.index->getCurrentMapsVariants());
}

/// For bidirectional hash join
/// There are 2 blocks returned : joined block via parameter and retracted block via returned-value if there is
Block HybridHashJoin::insertLeftBlockAndJoin(Block & left_block)
{
    chassert(bidirectional_hash_join && !range_bidirectional_hash_join);

    /// For bidirectional join, process one stream at a time
    std::scoped_lock lock(left_data.index->mutex, right_data.index->mutex);

    if (emitChangeLog())
    {
        /// If this is a retract block which contains _tp_delta with -1 as its value
        if (auto joined_retracted_block = eraseExistingKeysAndRetractJoin<true>(left_block); joined_retracted_block)
            return std::move(*joined_retracted_block);
    }
    else
    {
        /// Since we are not emitting changelog, just erase the existing keys from the left side hash map, no need to do retract join
        if (isRetractBlock(left_block, *left_data.join_ctx.join_stream_desc))
        {
            eraseExistingKeys<true>(left_block, left_data);
            left_block.clear();
            return {};
        }
    }

    /// For right/full join, if left data have new join keys, we need retract prev joined result (join key + right_cols + empty left_cols),
    /// for example: `lkv right join rkv on lkv.key = rkv.key`
    ///         (lkv)           (rkv)               (right joined)
    /// (t1)                    k1,v1,+1    >>      rkv.k1, rkv.v1, (columns with 'empty' values for lkv), +1
    /// (t2)    k1,v2,+1                    >>      rkv.k1, rkv.v1, (columns with 'empty' values for lkv), -1
    ///                                             k1, v2, rkv.k1, rkv.v1, +1
    /// So, at (t2): we need to do right join left with `rkv.k1,(other defaults),-1` and `k1,v2,+1`
    if ((streaming_kind == Kind::Right || streaming_kind == Kind::Full) && emitChangeLog())
    {
        IColumn::Filter new_keys_filter(left_block.rows(), 0);
        doInsertBlock<true>(left_block, left_data.index->getCurrentMapsVariants(), &new_keys_filter);
        chassert(left_data.join_ctx.join_stream_desc->hasDeltaColumn());

        Block retracted_block;
        size_t new_keys_rows = std::accumulate(new_keys_filter.begin(), new_keys_filter.end(), static_cast<size_t>(0));
        if (new_keys_rows > 0)
        {
            /// 1) Generate a block for the new keys
            MutableColumns columns;
            columns.reserve(left_block.columns());
            /// FIXME, multiple disjuncts OR clause
            const auto & key_names = table_join->getClauses().front().key_names_left;
            for (const auto & col_with_type_name : left_block)
            {
                auto is_key_column = std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; });
                if (is_key_column)
                {
                    columns.emplace_back(IColumn::mutate(col_with_type_name.column->filter(new_keys_filter, new_keys_rows)));
                }
                else if (col_with_type_name.name == left_data.join_ctx.join_stream_desc->deltaColumnName())
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

            /// 2) Do retract left block <inner join> right side
            retracted_block = left_block.cloneWithColumns(std::move(columns));
            auto pushdown_retracted
                = joinBlockWithHashTable<true>(retracted_block, right_data.index->getCurrentMapsVariants(), Kind::Inner);
            chassert(!pushdown_retracted);
            /// NOTE: Reset the right join key columns to default values after joined, for example:
            /// `rkv.k1, rkv.v1, lkv.k1, (lkv.other_defaults), -1`  =>  `rkv.k1, rkv.v1, (lkv.defaults), -1`
            auto retracted_rows = retracted_block.rows();
            if (retracted_rows)
            {
                for (auto & col_with_type_name : retracted_block)
                {
                    auto is_key_column = std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; });
                    if (is_key_column)
                    {
                        auto key_col = IColumn::mutate(col_with_type_name.column->cloneEmpty());
                        key_col->reserve(retracted_rows);
                        key_col->insertManyDefaults(retracted_rows);
                        col_with_type_name.column = std::move(key_col);
                    }
                }
            }
        }

        /// 3) Do left_side <right join> right_side
        auto pushdown_retracted = joinBlockWithHashTable<true>(left_block, right_data.index->getCurrentMapsVariants());
        chassert(!pushdown_retracted);

        return retracted_block;
    }

    doInsertBlock<true>(left_block, left_data.index->getCurrentMapsVariants());

    return joinBlockWithHashTable<true>(left_block, right_data.index->getCurrentMapsVariants());
}

Block HybridHashJoin::insertRightBlockAndJoin(Block & right_block)
{
    chassert(output_header);
    chassert(bidirectional_hash_join && !range_bidirectional_hash_join);

    std::scoped_lock lock(left_data.index->mutex, right_data.index->mutex);

    if (emitChangeLog())
    {
        /// If this is a retract block which contains _tp_delta with -1 as its value
        if (auto joined_retracted_block = eraseExistingKeysAndRetractJoin<false>(right_block); joined_retracted_block)
            return std::move(*joined_retracted_block);
    }
    else
    {
        /// Since we are not emitting changelog, just erase the existing keys from the left side hash map, no need to do retract join
        if (isRetractBlock(right_block, *right_data.join_ctx.join_stream_desc))
        {
            eraseExistingKeys<false>(right_block, right_data);
            right_block.clear();
            return {};
        }
    }

    /// For left join, if right data have new join keys, we need retract prev left joined result (join key + left_cols + empty right_cols),
    /// for example: `lkv left join rkv on lkv.key = rkv.key`
    ///         (lkv)           (rkv)               (left joined)
    /// (t1)    k1,v1,+1                    >>      k1, v1, (columns with 'empty' values for rkv), +1
    /// (t2)                    k1,v2,+1    >>      k1, v1, (columns with 'empty' values for rkv), -1
    ///                                             k1, v1, rkv.k1, rkv.v2, +1
    /// So, at (t2): we need to do right join left with `k1,(other defaults),-1` and `k1,v2,+1`
    if ((streaming_kind == Kind::Left || streaming_kind == Kind::Full) && emitChangeLog())
    {
        IColumn::Filter new_keys_filter(right_block.rows(), 0);
        doInsertBlock<false>(right_block, right_data.index->getCurrentMapsVariants(), &new_keys_filter);
        chassert(right_data.join_ctx.join_stream_desc->hasDeltaColumn());

        Block retracted_block;
        size_t new_keys_rows = std::accumulate(new_keys_filter.begin(), new_keys_filter.end(), static_cast<size_t>(0));
        if (new_keys_rows > 0)
        {
            /// 1) Generate a block for the new keys
            MutableColumns columns;
            columns.reserve(right_block.columns());
            /// FIXME, multiple disjuncts OR clause
            const auto & key_names = table_join->getClauses().front().key_names_right;
            for (const auto & col_with_type_name : right_block)
            {
                auto is_key_column = std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; });
                if (is_key_column)
                {
                    columns.emplace_back(IColumn::mutate(col_with_type_name.column->filter(new_keys_filter, new_keys_rows)));
                }
                else if (col_with_type_name.name == right_data.join_ctx.join_stream_desc->deltaColumnName())
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

            /// 2) Do retract right_side <inner join> left_side
            retracted_block = right_block.cloneWithColumns(std::move(columns));
            auto pushdown_retracted
                = joinBlockWithHashTable<false>(retracted_block, left_data.index->getCurrentMapsVariants(), Kind::Inner);
            chassert(!pushdown_retracted);
            /// NOTE: Reset the right join key columns to default values after joined, for example:
            /// `k1, v1, rkv.k1, (rkv.other_defaults), -1`  =>  `k1, v1, (rkv.defaults), -1`
            auto retracted_rows = retracted_block.rows();
            if (retracted_rows)
            {
                for (auto & col_with_type_name : retracted_block)
                {
                    auto is_key_column = std::ranges::any_of(key_names, [&](const auto & name) { return name == col_with_type_name.name; });
                    if (is_key_column)
                    {
                        auto key_col = IColumn::mutate(col_with_type_name.column->cloneEmpty());
                        key_col->reserve(retracted_rows);
                        key_col->insertManyDefaults(retracted_rows);
                        col_with_type_name.column = std::move(key_col);
                    }
                }
            }
        }

        /// 3) Do right_side <left join> left_side
        auto pushdown_retracted = joinBlockWithHashTable<false>(right_block, left_data.index->getCurrentMapsVariants());
        chassert(!pushdown_retracted);

        return retracted_block;
    }
    else
    {
        doInsertBlock<false>(right_block, right_data.index->getCurrentMapsVariants());
        return joinBlockWithHashTable<false>(right_block, left_data.index->getCurrentMapsVariants());
    }
}

/// For bidirectional range hash join, there may be multiple joined blocks
std::vector<Block> HybridHashJoin::insertLeftBlockToRangeBucketsAndJoin(Block left_block)
{
    auto joined_blocks = insertBlockToRangeBucketsAndJoin<true>(std::move(left_block));
    for (auto & block : joined_blocks)
        transformToOutputBlock<true>(block);

    return joined_blocks;
}

std::vector<Block> HybridHashJoin::insertRightBlockToRangeBucketsAndJoin(Block right_block)
{
    chassert(output_header);

    auto joined_blocks = insertBlockToRangeBucketsAndJoin<false>(std::move(right_block));

    for (auto & block : joined_blocks)
        transformToOutputBlock<false>(block);

    return joined_blocks;
}

template <bool is_left_block>
std::vector<Block> HybridHashJoin::insertBlockToRangeBucketsAndJoin(Block block)
{
    chassert(block.rows());
    chassert(range_bidirectional_hash_join);

    auto * joining_data = &left_data;
    auto * joined_data = &right_data;
    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    /// Here we explicitly order the lock of the mutex to avoid deadlock
    std::scoped_lock lock(left_data.index->mutex, right_data.index->mutex);

    /// Insert
    auto bucket_blocks = joining_data->index->assignDataBlockToRangeBuckets(std::move(block));
    for (auto & bucket_block : bucket_blocks)
        /// Here we copy over the block since the block will be used to join which will modify the columns in-place
        doInsertBlock<is_left_block>(bucket_block.block, *bucket_block.maps_variants);

    std::vector<Block> joined_blocks;
    joined_blocks.reserve(bucket_blocks.size());

    /// Join
    for (auto & bucket_block : bucket_blocks)
    {
        auto joining_bucket = static_cast<Int64>(bucket_block.bucket);

        /// Find the range buckets to join
        std::vector<HybridHashJoinMapsVariants *> joined_hash_indexes_ptrs;
        auto & joined_range_bucket_hash_indexes = joined_data->index->getRangeBucketHashIndexes();

        auto bucket_offset = joining_data->index->join_start_bucket_offset;
        if constexpr (!is_left_block)
            bucket_offset = joining_data->index->join_stop_bucket_offset;

        auto lower_bound = joining_bucket - bucket_offset;
        auto joined_range_bucket_iter = joined_range_bucket_hash_indexes.lower_bound(lower_bound);
        if (joined_range_bucket_iter != joined_range_bucket_hash_indexes.end())
        {
            bucket_offset = joining_data->index->join_stop_bucket_offset;
            if constexpr (!is_left_block)
                bucket_offset = joining_data->index->join_start_bucket_offset;
            auto upper_bound = joining_bucket + bucket_offset;

            for (auto joined_range_bucket_end = joined_range_bucket_hash_indexes.end(); joined_range_bucket_iter != joined_range_bucket_end;
                 ++joined_range_bucket_iter)
            {
                if (joined_range_bucket_iter->first > upper_bound)
                    /// Reaching the upper bound of right bucket to join
                    break;

                auto & joined_bucket_index = joined_range_bucket_iter->second;

                /// Although we are bucketing blocks, but the real min/max in 2 buckets may not be join-able
                /// If [min, max] of the joining bucket doesn't intersect with joined range bucket as a whole,
                /// we are sure there will be no join-able rows for the whole bucket
                bool has_intersect = false;
                if constexpr (is_left_block)
                    has_intersect = joining_data->index->intersect(
                        bucket_block.block.minTimestamp(),
                        bucket_block.block.maxTimestamp(),
                        joined_bucket_index.min_ts,
                        joined_bucket_index.max_ts);
                else
                    has_intersect = joining_data->index->intersect(
                        joined_bucket_index.min_ts,
                        joined_bucket_index.max_ts,
                        bucket_block.block.minTimestamp(),
                        bucket_block.block.maxTimestamp());

                if (!has_intersect)
                    continue;

                joined_hash_indexes_ptrs.emplace_back(joined_bucket_index.index.get());
            }
        }

        auto & join_block = bucket_block.block;
        doJoinBlockWithHashTables<is_left_block>(join_block, joined_hash_indexes_ptrs, streaming_kind);

        if (join_block.rows())
            joined_blocks.emplace_back(std::move(join_block));
    }

    calculateWatermark();

    left_data.index->removeOldBuckets("left_stream");
    right_data.index->removeOldBuckets("right_stream");

    return joined_blocks;
}

template <bool is_left_block>
void HybridHashJoin::doInsertBlock(Block block, HybridHashJoinMapsVariants & target_maps_variants, IColumn::Filter * new_keys_filter)
{
    /// FIXME, there are quite some block copies
    /// FIXME, all_key_columns shall hold shared_ptr to columns instead of raw ptr
    /// then we can update `source_block` in place
    JoinTableSide side = JoinTableSide::Left;
    if constexpr (!is_left_block)
        side = JoinTableSide::Right;

    /// key columns are from source `block`
    const auto & right_key_names = table_join->getAllNames(side);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
    {
        const auto & column = block.getByName(column_name).column;
        all_key_columns[column_name] = recursiveRemoveSparse(column->convertToFullColumnIfConst())->convertToFullColumnIfLowCardinality();
    }

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
        key_columns.push_back(all_key_columns[name].get());

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    /// Add `block_to_save` to target stream data
    /// Note `block_to_save` may be empty for cases in which the query doesn't care other non-key columns.
    /// For example, SELECT count() FROM stream_a JOIN stream_b ON i=ii;
    hybridJoinDispatch(
        streaming_kind, streaming_strictness, target_maps_variants[0], [&, this](auto /*kind_*/, auto strictness_, auto & map) {
            [[maybe_unused]] size_t size = insertFromBlockImpl<strictness_>(
                *this, hash_method_type, map, key_columns, key_sizes[0], block_to_save, null_map, new_keys_filter);
        });

    checkLimits();
}

template <bool is_left_block>
std::optional<Block> HybridHashJoin::eraseExistingKeysAndRetractJoin(Block & block)
{
    chassert(bidirectional_hash_join);

    JoinData * joining_data = &left_data;
    JoinData * joined_data = &right_data;

    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    if (!isRetractBlock(block, *joining_data->join_ctx.join_stream_desc))
        return {};

    /// First erase the keys
    /// FIXME, avoid the copy
    Block copy_block = block;
    eraseExistingKeys<is_left_block>(copy_block, *joining_data);

    /// Then do retract join
    doJoinBlockWithHashTable<is_left_block>(block, joined_data->index->getCurrentMapsVariants());

    transformToOutputBlock<is_left_block>(block);

    /// Even empty block, we will need move back to indicate this is a retract block and we have processed it
    return std::move(block);
}

template <bool is_left_block>
void HybridHashJoin::eraseExistingKeys(Block & block, JoinData & join_data)
{
    /// For LATEST / ASOF join, we don't need retract, just drop the retract block on the floor
    /// Actually, it is better to avoid the ChangelogTransform entirely for versioned-kv case.
    /// One challenge to drop the ChangelogTransform for versioned-kv is we will need first
    /// evaluate the whole join semantic first in InterpreterSelectQuery. For multiple join,
    /// it would be a bit difficult
    if (streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Latest)
        return;

    /// Find previous key / values on join columns
    const auto & key_column_names = table_join->getAllNames(is_left_block ? JoinTableSide::Left : JoinTableSide::Right);
    ColumnPtrMap all_key_columns(key_column_names.size());
    for (const auto & column_name : key_column_names)
    {
        const auto & column = block.getByName(column_name).column;
        all_key_columns[column_name] = recursiveRemoveSparse(column->convertToFullColumnIfConst())->convertToFullColumnIfLowCardinality();
    }

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
        key_columns.push_back(all_key_columns[name].get());

    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    Block saved_block = prepareBlock<is_left_block>(block);
    chassert(join_data.join_ctx.reserved_column_positions);

    auto & map_variant = join_data.index->getCurrentMapsVariants()[0];
    const auto & key_size = key_sizes[0];
    auto delete_key = isChangelogKVStorage(join_data.join_ctx.join_stream_desc->data_stream_semantic);

    hybridJoinDispatch(streaming_kind, streaming_strictness, map_variant, [&, this](auto, auto, auto & tagged_map) {
        using TaggedType = std::decay_t<decltype(tagged_map)>::TaggedType;
        switch (hash_method_type)
        {
            default:
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invalid hash type='{}' chosen for hybrid hash join",
                    magic_enum::enum_name(hash_method_type));
            }

#define M(TYPE, IS_TWO_LEVEL) \
    case HybridHashType::TYPE: \
    { \
        using KeyGetter = HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/false>; \
        doEraseExistingKeys<KeyGetter, TaggedType>( \
            *tagged_map.table.TYPE, \
            saved_block, \
            std::move(key_columns), \
            key_size, \
            *join_data.join_ctx.reserved_column_positions, \
            null_map, \
            delete_key); \
        break; \
    }
                APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_JOIN_HYBRID(M)
#undef M
        }
    });
}

template <typename KeyGetter, typename TaggedType, typename Map>
void HybridHashJoin::doEraseExistingKeys(
    Map & map,
    const DB::Block & saved_block,
    ColumnRawPtrs && key_columns,
    const std::vector<size_t> & key_size,
    const std::vector<size_t> & skip_columns,
    ConstNullMapPtr null_map,
    bool delete_key)
{
    KeyGetter key_getter(std::move(key_columns), key_size);

    if constexpr (!std::is_same_v<TaggedType, RowList>)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Map value is expected to be RowList");

    for (size_t row = 0, rows = saved_block.rows(); row < rows; ++row)
    {
        if (null_map && (*null_map)[row])
            continue;

        auto key = key_getter.getKeyHolder(row);
        auto find_result = map.findKey(key, /*disable_spill=*/true);
        if (find_result.hasError())
            throw Exception::createRuntime(find_result.errcode, find_result.errorString());

        if (find_result.isFound())
        {
            /// Loop the value list to find a match on other values
            bool found = false;
            auto * mapped = static_cast<RowList *>(find_result.getMutableMapped());
            for (auto iter = mapped->rows.begin(), iter_end = mapped->rows.end(); iter != iter_end; ++iter)
            {
                /// Compare each column except the delta column in the indexed block with retract block
                if (compareEqual(saved_block, row, *iter, skip_columns))
                {
                    mapped->rows.erase(iter);
                    found = true;
                    break;
                }
            }

            if (!found)
                /// Key not exist. Out of order
                throw Exception(ErrorCodes::INTERNAL_ERROR, "Existing value is not found in joined hash table");

            if (mapped->empty() && delete_key)
                map.removeKey(key);
        }
        else
        {
            /// Key not exist. Out of order
            throw Exception(ErrorCodes::INTERNAL_ERROR, "Existing key is not found in joined hash table");
        }
    }

    /// In erase loop, we disable spill and we do bulk spill here
    map.spillIfNecessary();
}

void HybridHashJoin::joinBlock(Block & block, [[maybe_unused]] ExtraBlockPtr & not_processed)
{
    joinLeftBlock(block);
}

/// Use left_block to join right hash table
void HybridHashJoin::joinLeftBlock(Block & left_block)
{
    chassert(!bidirectional_hash_join && !range_bidirectional_hash_join);

    /// SELECT * FROM append_only [INNER | LEFT | RIGHT | FULL] JOIN versioned_kv
    /// SELECT * FROM append_only ASOF JOIN versioned_kv
    /// SELECT * FROM append_only ASOF JOIN right_append_only

    std::scoped_lock lock(right_data.index->mutex);

    doJoinBlockWithHashTable<true>(left_block, right_data.index->getCurrentMapsVariants());
    transformToOutputBlock<true>(left_block);
}

/// Join left block with right hash table or join right block with left hash table
template <bool is_left_block, Kind KIND, Strictness STRICTNESS, typename TaggedMap>
void HybridHashJoin::joinBlockImpl(
    Block & block, const Block & block_with_columns_to_add, std::vector<std::vector<TaggedMap *>> & map_vv) const
{
    /// joining -> the "left" side
    /// joined -> the "right" side
    /// When `is_left_block = false`, `joining` is actually the `right` stream, and `joined` is actually the `left` stream
    const JoinData * joining_data = &left_data;
    const JoinData * joined_data = &right_data;

    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    constexpr JoinFeatures<KIND, STRICTNESS, HybridMapGetter> jf;

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
        block,
        block_with_columns_to_add,
        *joined_sample_block,
        *this,
        std::move(join_on_keys),
        joining_data->index->range_asof_join_ctx,
        jf.is_asof_join || jf.is_range_join,
        is_left_block);

    bool has_required_joined_keys = (joined_data->join_ctx.required_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_joined_keys;

    bool has_null_map
        = std::any_of(added_columns.join_on_keys.begin(), added_columns.join_on_keys.end(), [](const auto & k) { return k.null_map; });

    IColumn::Filter row_filter = has_null_map
        ? switchJoinColumns<KIND, STRICTNESS, /*has_null_map==*/true>(map_vv, added_columns, hash_method_type)
        : switchJoinColumns<KIND, STRICTNESS, /*has_null_map==*/false>(map_vv, added_columns, hash_method_type);

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
        for (size_t i = 0, columns = joined_data->join_ctx.required_keys.columns(); i < columns; ++i)
        {
            const auto & joined_key = joined_data->join_ctx.required_keys.getByPosition(i);
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

                const auto & col = block.getByName(joined_data->join_ctx.required_keys_sources[i]);

                /// We need rename right column name if `is_left_block is true`
                /// Otherwise keep the column name as it is as column from actual left block don't require a renaming
                const auto & joined_key_name = is_left_block ? getTableJoin().renamedRightColumnName(joined_key.name) : joined_key.name;
                auto joined_key_col = copyLeftKeyColumnToRight(joined_key.type, joined_key_name, col);
                block.insert(std::move(joined_key_col));
            }
        }
    }
    else if (has_required_joined_keys)
    {
        /// Add joined key columns from joined block if needed.
        for (size_t i = 0, columns = joined_data->join_ctx.required_keys.columns(); i < columns; ++i)
        {
            const auto & joined_key = joined_data->join_ctx.required_keys.getByPosition(i);
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

                const auto & col = block.getByName(joined_data->join_ctx.required_keys_sources[i]);
                auto joined_key_col = copyLeftKeyColumnToRight(joined_key.type, joined_col_name, col, &row_filter);
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

template <bool is_left_block>
Block HybridHashJoin::joinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants)
{
    return joinBlockWithHashTable<is_left_block>(block, maps_variants, streaming_kind);
}

template <bool is_left_block>
Block HybridHashJoin::joinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants, Kind join_kind)
{
    chassert(bidirectional_hash_join || range_bidirectional_hash_join);

    doJoinBlockWithHashTable<is_left_block>(block, maps_variants, join_kind);

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

template <bool is_left_block>
void HybridHashJoin::doJoinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants)
{
    doJoinBlockWithHashTable<is_left_block>(block, maps_variants, streaming_kind);
}

template <bool is_left_block>
void HybridHashJoin::doJoinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants, Kind join_kind)
{
    std::vector<HybridHashJoinMapsVariants *> maps_variants_v{&maps_variants};
    doJoinBlockWithHashTables<is_left_block>(block, maps_variants_v, join_kind);
}

template <bool is_left_block>
void HybridHashJoin::doJoinBlockWithHashTables(Block & block, std::vector<HybridHashJoinMapsVariants *> & maps_variants_v, Kind join_kind)
{
    JoinData * joining_data = &left_data;
    JoinData * joined_data = &right_data;
    if constexpr (!is_left_block)
        std::swap(joining_data, joined_data);

    if (unlikely(!joining_data->join_ctx.validated_join_key_types))
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
                joined_data->join_ctx.join_stream_desc->input_header,
                is_left_block ? onexpr.key_names_right : onexpr.key_names_left,
                is_left_block ? cond_column_name.second : cond_column_name.first);
        }

        joining_data->join_ctx.validated_join_key_types = true;
    }

    /// If there are no joined hash tables, we may need to add missing columns to the block for left or full join.
    if (maps_variants_v.empty())
    {
        if ((is_left_block && join_kind == Kind::Left) || join_kind == Kind::Full)
            addMissingColumns<is_left_block>(block);
        else
            block.clear();

        return;
    }

    chassert(!maps_variants_v.empty());
    using Map = std::decay_t<decltype(maps_variants_v.front()->front())>;
    /// Convert \maps_variants_v
    /// From: multiple buckets > multiple disjuncts
    /// To  : multiple disjuncts + multiple bucket hash tables per disjunct (only range join has multiple hash tables), for example:
    /// 1) for range bucket join:
    ///  [
    ///    [bucket-hash-table-1, bucket-hash-table-2]  (disjunct-1)
    ///    [bucket-hash-table-3, bucket-hash-table-4]  (disjunct-2)
    ///    [bucket-hash-table-5, bucket-hash-table-6]  (disjunct-3)
    ///  ]
    /// 2) for non range bucket join:
    ///  [
    ///    [hash-table]  (disjunct-1)
    ///    [hash-table]  (disjunct-2)
    ///    [hash-table]  (disjunct-3)
    ///  ]
    std::vector<std::vector<const Map *>> map_vv;
    map_vv.resize(table_join->getClauses().size());
    for (size_t j = 0; j < maps_variants_v.size(); ++j)
    {
        for (size_t i = 0; i < table_join->getClauses().size(); ++i)
            map_vv[i].emplace_back(&(*(maps_variants_v[j]))[i]);
    }

    auto flipped_kind = join_kind;
    if constexpr (!is_left_block)
        flipped_kind = flipKind(join_kind);

    if (hybridJoinDispatch(flipped_kind, streaming_strictness, map_vv, [&](auto kind_, auto strictness_, auto & map_vv_) {
            joinBlockImpl<is_left_block, kind_, strictness_>(block, joined_data->join_ctx.sample_block_with_columns_to_add, map_vv_);
        }))
    {
        /// Joined
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
    }
}


template <bool is_left_block>
void HybridHashJoin::transformToOutputBlock(Block & joined_block) const
{
    /// Please note we didn't reorder columns according to output header if block is empty to save some cpu cycles
    /// Caller shall check if the retracted block is empty and avoid pushing this empty block downstream since
    /// this empty block's structure probably doesn't match the output header
    /// Also for stream join hybrid table
    if (!joined_block.rows() || !right_data.join_ctx.join_stream_desc->data_stream_semantic.streaming)
        return;

    if constexpr (is_left_block)
    {
        /// For exmaple: c1(i, v, _tp_delta) join c2(i, v, _tp_delta)
        /// (left join right)
        /// joined block:   "i, v, _tp_delta, c2.i, c2.v, c2._tp_delta"
        if (emitChangeLog())
        {
            /// output header:  "i, v, c2.i, c2.v, _tp_delta"
            chassert(left_delta_column_position_lrj && right_delta_column_position_lrj);

            /// At first, remove right delta column (from back to front)
            chassert(*right_delta_column_position_lrj > *left_delta_column_position_lrj);
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
            chassert(left_delta_column_position_rlj && right_delta_column_position_rlj);

            /// At first, move right delta column to left delta column
            auto & right_retract_delta_col = joined_block.getByPosition(*right_delta_column_position_rlj);
            auto & left_delta_col = joined_block.getByPosition(*left_delta_column_position_rlj);
            left_delta_col.column = std::move(right_retract_delta_col.column);

            /// Then remove right delta column, skip this operation since next reordering will ignore it
        }
        else
        {
            /// output header:  "c2.i, c2.v, i, v"
            /// Remove left or right delta columns, skip this operation since next reordering will ignore it
        }

        joined_block.reorderColumnsInplace(output_header);
    }
}

void HybridHashJoin::transformHeader(Block & header)
{
    if (range_bidirectional_hash_join)
        joinBlockWithHashTable<true>(header, right_data.index->getCurrentMapsVariants());
    else if (bidirectional_hash_join)
        joinBlockWithHashTable<true>(header, right_data.index->getCurrentMapsVariants());
    else
        joinLeftBlock(header);

    /// Doesn't handle _tp_delta for stream join hybrid table
    if (!right_data.join_ctx.join_stream_desc->data_stream_semantic.streaming)
        return;

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
    chassert(!header.has(ProtonConsts::RESERVED_DELTA_FLAG));

    /// Append joined delta column
    if (emitChangeLog())
        header.insert({DataTypeFactory::instance().get(TypeIndex::Int8), ProtonConsts::RESERVED_DELTA_FLAG});
}

void HybridHashJoin::calculateWatermark()
{
    Int64 left_watermark = left_data.index->current_watermark;
    Int64 right_watermark = right_data.index->current_watermark;

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

void HybridHashJoin::postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_)
{
    if (output_header)
        return;

    join_max_cached_bytes = join_max_cached_bytes_;
    left_data.join_ctx.join_stream_desc->input_header = left_header;
    output_header = output_header_;

    /// Now, we can evaluate the primary key column positions for left join stream desc since left header is ready
    left_data.join_ctx.join_stream_desc->calculateColumnPositions(strictness);

    /// initLeftPrimaryKeyHashTable();

    validateAsofJoinKey();

    /// If it is not bidirectional hash join, we don't care left header
    if (bidirectional_hash_join)
    {
        /// left stream sample block's column may be not inited yet
        JoinCommon::createMissedColumns(left_data.join_ctx.join_stream_desc->input_header);
        if (table_join->oneDisjunct())
        {
            const auto & key_names_left = table_join->getOnlyClause().key_names_left;
            JoinCommon::splitAdditionalColumns(
                key_names_left,
                left_data.join_ctx.join_stream_desc->input_header,
                left_data.join_ctx.table_keys,
                left_data.join_ctx.sample_block_with_columns_to_add);
            left_data.join_ctx.required_keys
                = table_join->getRequiredLeftKeys(left_data.join_ctx.table_keys, left_data.join_ctx.required_keys_sources);
        }
        else
        {
            left_data.join_ctx.sample_block_with_columns_to_add = left_data.join_ctx.table_keys
                = materializeBlock(left_data.join_ctx.join_stream_desc->input_header);
        }

        initLeftBlockStructure();
        /// left sample block now was inited, set the sample block to HashIndex now
        left_data.index->sample_block = left_data.join_ctx.sample_block;

        initHashMaps(left_data.index->getCurrentMapsVariants(), left_data.join_ctx.sample_block, left_data.index->id);

        if (retract_push_down && emit_changelog)
        {
            join_results.emplace(output_header);
            /// FIXME output_header - keys columns
            initHashMaps(*join_results->index, output_header, "retracts");
        }
    }

    if (left_data.join_ctx.join_stream_desc->hasDeltaColumn() || right_data.join_ctx.join_stream_desc->hasDeltaColumn())
    {
        /// If it is not bidirectional hash join, we don't care `right join left`
        if (bidirectional_hash_join)
        {
            /// 1) We'd like to compute some reserved column positions for `the right block join left block` case
            /// since we will need swap them before project the join results
            Block right_join_left_header = right_data.join_ctx.join_stream_desc->input_header;
            doJoinBlockWithHashTable<false>(right_join_left_header, left_data.index->getCurrentMapsVariants());

            for (size_t i = 0; const auto & col : right_join_left_header)
            {
                if (col.name == ProtonConsts::RESERVED_DELTA_FLAG)
                    left_delta_column_position_rlj = i;
                else if (col.name.ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
                    right_delta_column_position_rlj = i;

                ++i;
            }
        }

        /// 2) We'd like to compute some reserved column positions for `the left block join right block` case
        /// since we will need swap them before project the join results
        Block left_join_right_header = left_data.join_ctx.join_stream_desc->input_header;
        doJoinBlockWithHashTable<true>(left_join_right_header, right_data.index->getCurrentMapsVariants());
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

void HybridHashJoin::validateAsofJoinKey()
{
    auto maybe_asof_join_col_info = doValidateAsofJoinKey();
    if (!maybe_asof_join_col_info)
        return;

    const auto & asof_col = maybe_asof_join_col_info.value();
    left_data.index->updateAsofJoinColumnPositionAndScale(asof_col.scale, asof_col.left_col_pos, asof_col.type_index);
    right_data.index->updateAsofJoinColumnPositionAndScale(asof_col.scale, asof_col.right_col_pos, asof_col.type_index);
}

}

}
