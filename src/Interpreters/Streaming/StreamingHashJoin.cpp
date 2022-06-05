#include "StreamingHashJoin.h"

#include <base/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/DictionaryReader.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/join_common.h>

#include <Storages/StorageDictionary.h>

#include <Core/ColumnNumbers.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <any>
#include <limits>
#include <unordered_map>

/// proton : starts
#include <Core/Block.h>
#include <Common/ProtonCommon.h>
/// proton : ends

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

    template <StreamingHashJoin::Type type, typename Value, typename Mapped>
    struct KeyGetterForTypeImpl;

    constexpr bool use_offset = true;

    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key8, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key16, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key32, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key64, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key_string, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::key_fixed_string, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::keys128, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::keys256, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
    };
    template <typename Value, typename Mapped>
    struct KeyGetterForTypeImpl<StreamingHashJoin::Type::hashed, Value, Mapped>
    {
        using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
    };

    template <StreamingHashJoin::Type type, typename Data>
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
            const StreamingHashJoin & join,
            std::vector<JoinOnKeyColumns> && join_on_keys_,
            JoinTupleMap * joined_rows_,
            const RangeAsofJoinContext & range_join_ctx_,
            bool is_asof_join)
            : join_on_keys(join_on_keys_)
            , rows_to_add(block.rows())
            , joined_rows(joined_rows_)
            , range_join_ctx(range_join_ctx_)
            , src_block_id(block.info.blockId())
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
        /// ASOF::Inequality asofInequality() const { return asof_inequality; }
        const IColumn & leftAsofKey() const { return *left_asof_key; }

        std::vector<JoinOnKeyColumns> join_on_keys;

        size_t rows_to_add;
        std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
        bool need_filter = false;

        /// proton : starts
        JoinTupleMap * joined_rows;
        const RangeAsofJoinContext & range_join_ctx;
        UInt64 src_block_id;
        /// proton : ends

    private:
        std::vector<TypeAndName> type_name;
        MutableColumns columns;
        std::vector<size_t> right_indexes;
        size_t lazy_defaults_count = 0;
        /// for ASOF
        std::optional<TypeIndex> asof_type;
        ASOF::Inequality asof_inequality;
        const IColumn * left_asof_key = nullptr;

        void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
        {
            columns.push_back(src_column.column->cloneEmpty());
            columns.back()->reserve(src_column.column->size());
            type_name.emplace_back(src_column.type, src_column.name, qualified_name);
        }
    };

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    struct JoinFeatures
    {
        static constexpr bool is_any_join = STRICTNESS == ASTTableJoin::Strictness::Any;
        static constexpr bool is_any_or_semi_join = STRICTNESS == ASTTableJoin::Strictness::Any
            || STRICTNESS == ASTTableJoin::Strictness::RightAny
            || (STRICTNESS == ASTTableJoin::Strictness::Semi && KIND == ASTTableJoin::Kind::Left);
        static constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
        static constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
        static constexpr bool is_range_asof_join = STRICTNESS == ASTTableJoin::Strictness::RangeAsof;
        static constexpr bool is_range_join = STRICTNESS == ASTTableJoin::Strictness::Range;
        static constexpr bool is_semi_join = STRICTNESS == ASTTableJoin::Strictness::Semi;
        static constexpr bool is_anti_join = STRICTNESS == ASTTableJoin::Strictness::Anti;

        static constexpr bool left = KIND == ASTTableJoin::Kind::Left;
        static constexpr bool right = KIND == ASTTableJoin::Kind::Right;
        static constexpr bool inner = KIND == ASTTableJoin::Kind::Inner;
        static constexpr bool full = KIND == ASTTableJoin::Kind::Full;

        static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right) || is_range_join;
        static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));
        static constexpr bool add_missing = (left || full) && !is_semi_join;

        static constexpr bool need_flags = MapGetter<KIND, STRICTNESS>::flagged;
    };

    template <typename Map, bool add_missing>
    bool addFoundRowAll(const typename Map::mapped_type & mapped, AddedColumns & added_columns, IColumn::Offset & current_offset, uint32_t row_num_in_left_block)
    {
        if constexpr (add_missing)
            added_columns.applyLazyDefaults();

        bool added = false;
        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            auto result = added_columns.joined_rows->insert(JoinTuple{added_columns.src_block_id, it->block, row_num_in_left_block, it->row_num});
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
    template <
        ASTTableJoin::Kind KIND,
        ASTTableJoin::Strictness STRICTNESS,
        typename KeyGetter,
        typename Map,
        bool need_filter,
        bool has_null_map,
        bool multiple_disjuncts>
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
                        /// FIXME
                    }
                    else if constexpr (jf.is_all_join)
                    {
                        if (addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, i))
                            setUsed<need_filter>(filter, i);
                    }
                    else if constexpr ((jf.is_any_join || jf.is_semi_join) && jf.right)
                    {
                        /// FIXME
                    }
                    else if constexpr (jf.is_any_join && KIND == ASTTableJoin::Kind::Inner)
                    {
                        break;
                    }
                    else if constexpr (jf.is_any_join && jf.full)
                    {
                        /// TODO
                    }
                    else if constexpr (jf.is_anti_join)
                    {
                        /// FIXME
                    }
                    else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
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
            {
                if constexpr (jf.is_anti_join && jf.left)
                    setUsed<need_filter>(filter, i);
                addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
            }

            if constexpr (jf.need_replication)
            {
                (*added_columns.offsets_to_replicate)[i] = current_offset;
            }
        }

        added_columns.applyLazyDefaults();
        return filter;
    }

    template <
        ASTTableJoin::Kind KIND,
        ASTTableJoin::Strictness STRICTNESS,
        typename KeyGetter,
        typename Map,
        bool need_filter,
        bool has_null_map>
    IColumn::Filter joinRightColumnsSwitchMultipleDisjuncts(
        std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
    {
        return mapv.size() > 1 ? joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, true, true>(
                   std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns)
                               : joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, true, false>(
                                   std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    IColumn::Filter joinRightColumnsSwitchNullability(
        std::vector<KeyGetter> && key_getter_vector, const std::vector<const Map *> & mapv, AddedColumns & added_columns)
    {
        bool has_null_map
            = std::any_of(added_columns.join_on_keys.begin(), added_columns.join_on_keys.end(), [](const auto & k) { return k.null_map; });
        if (added_columns.need_filter)
        {
            if (has_null_map)
                return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, true>(
                    std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
            else
                return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, false>(
                    std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        }
        else
        {
            if (has_null_map)
                return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, true>(
                    std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
            else
                return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, false>(
                    std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns);
        }
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    IColumn::Filter
    switchJoinRightColumns(const std::vector<const Maps *> & mapv, AddedColumns & added_columns, StreamingHashJoin::Type type)
    {
        constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof || STRICTNESS == ASTTableJoin::Strictness::Range || STRICTNESS == ASTTableJoin::Strictness::RangeAsof;

        switch (type)
        {
#define M(TYPE) \
    case StreamingHashJoin::Type::TYPE: { \
        using MapTypeVal = const typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<StreamingHashJoin::Type::TYPE, MapTypeVal>::Type; \
        std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d) \
        { \
            const auto & join_on_key = added_columns.join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.push_back(std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes))); \
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
        static ALWAYS_INLINE void
        insertOne(const StreamingHashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }

        static ALWAYS_INLINE void
        insertAll(const StreamingHashJoin &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
            else
            {
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.getMapped().insert({stored_block, i}, pool);
            }
        }

        static ALWAYS_INLINE void insertAsof(
            StreamingHashJoin & join,
            Map & map,
            KeyGetter & key_getter,
            Block * stored_block,
            size_t i,
            Arena & pool,
            const IColumn & asof_column)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);
            typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

            TypeIndex asof_type = *join.getAsofType();
            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);
            time_series_map->insert(asof_type, asof_column, stored_block, i);
        }
    };

    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    size_t NO_INLINE insertFromBlockImplTypeCase(
        StreamingHashJoin & join,
        Map & map,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        Block * stored_block,
        ConstNullMapPtr null_map,
        UInt8ColumnDataPtr join_mask,
        Arena & pool)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRef>;
        constexpr bool is_asof_join = (STRICTNESS == ASTTableJoin::Strictness::Asof) || (STRICTNESS == ASTTableJoin::Strictness::RangeAsof)
                                      || (STRICTNESS == ASTTableJoin::Strictness::Range);

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = key_columns.back();

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            /// Check condition for right table from ON section
            if (join_mask && !(*join_mask)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, *asof_column);
            else if constexpr (mapped_one)
                Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
        return map.getBufferSizeInCells();
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    size_t insertFromBlockImplType(
        StreamingHashJoin & join,
        Map & map,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        Block * stored_block,
        ConstNullMapPtr null_map,
        UInt8ColumnDataPtr join_mask,
        Arena & pool)
    {
        if (null_map)
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(
                join, map, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool);
        else
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(
                join, map, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool);
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t insertFromBlockImpl(
        StreamingHashJoin & join,
        StreamingHashJoin::Type type,
        Maps & maps,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        Block * stored_block,
        ConstNullMapPtr null_map,
        UInt8ColumnDataPtr join_mask,
        Arena & pool)
    {
        switch (type)
        {
            case StreamingHashJoin::Type::EMPTY:
                assert(false);
                return 0;
            case StreamingHashJoin::Type::CROSS:
                assert(false);
                return 0; /// Do nothing. We have already saved block, and it is enough.
            case StreamingHashJoin::Type::DICT:
                assert(false);
                return 0; /// No one should call it with Type::DICT.

    #define M(TYPE) \
        case StreamingHashJoin::Type::TYPE: \
            return insertFromBlockImplType< \
                STRICTNESS, \
                typename KeyGetterForType<StreamingHashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool); \
            break;
            APPLY_FOR_JOIN_VARIANTS(M)
    #undef M
        }
        __builtin_unreachable();
        }
}

StreamingHashJoin::StreamingHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : table_join(std::move(table_join_))
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , right_sample_block(right_sample_block_)
    , log(&Poco::Logger::get("StreamingHashJoin"))
{
    /// if (strictness != ASTTableJoin::Strictness::RangeAsof && strictness != ASTTableJoin::Strictness::Range)
    ///     throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream to stream join requires range or range asof join");

    /// We support this combination
    /// 1. Range + inner + one disjunct join clause
    /// 2. All + left / inner + one or more disjunct join clause
    if (strictness != ASTTableJoin::Strictness::Range && strictness != ASTTableJoin::Strictness::All)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `range / all` joins are supported in stream to stream join");

    if (kind != ASTTableJoin::Kind::Left && kind != ASTTableJoin::Kind::Inner)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `inner / left` type join are supported in stream to stream join");

    if (!table_join->oneDisjunct())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream to Stream join only supports only one disjunct join clause");

    LOG_DEBUG(log, "Right sample block: {}", right_sample_block.dumpStructure());

    initData();

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, sample_block_with_columns_to_add);
        required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        /// sample_block_with_columns_to_add = right_table_keys = materializeBlock(right_sample_block);
    }

    LOG_TRACE(
        log,
        "Columns to add: [{}], required right [{}]",
        sample_block_with_columns_to_add.dumpStructure(),
        fmt::join(required_right_keys.getNames(), ", "));
    {
        std::vector<String> log_text;
        for (const auto & clause : table_join->getClauses())
            log_text.push_back(clause.formatDebug());
        LOG_TRACE(log, "Joining on: {}", fmt::join(log_text, " | "));
    }

    JoinCommon::convertToFullColumnsInplace(right_table_keys);
    initRightBlockStructure(right_data->sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(sample_block_with_columns_to_add);

    size_t disjuncts_num = table_join->getClauses().size();
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

        if (strictness == ASTTableJoin::Strictness::Range || strictness == ASTTableJoin::Strictness::RangeAsof)
        {
            assert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception("Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported", ErrorCodes::NOT_IMPLEMENTED);

            if (key_columns.size() <= 1)
                throw Exception("ASOF join needs at least one equal-join column", ErrorCodes::SYNTAX_ERROR);

            if (right_table_keys.getByName(key_names_right.back()).type->isNullable())
                throw Exception("ASOF join over right stream Nullable column is not implemented", ErrorCodes::NOT_IMPLEMENTED);

            size_t asof_size;
            asof_type = RangeAsofRowRefs::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            right_data->type = chooseMethod(key_columns, asof_key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            right_data->type = chooseMethod(key_columns, key_sizes.emplace_back());
        }
    }

    /// We will need init a dummy current_right_join_blocks here since during query plan,
    /// joinBlock() will be called to evaluate the header
    right_data->current_join_blocks = std::make_shared<RightTableData::RightTableBlocks>(right_data.get());
    left_data->current_join_blocks = std::make_shared<LeftTableData::LeftTableBlocks>(left_data.get());

    initHashMaps(right_data->current_join_blocks->maps);

    if (strictness == ASTTableJoin::Strictness::Range || strictness == ASTTableJoin::Strictness::RangeAsof)
        LOG_INFO(
            log,
            "Range join: {} join_start_bucket={} join_stop_bucket={}",
            table_join->rangeAsofJoinContext().string(),
            left_data->join_start_bucket,
            left_data->join_stop_bucket);
}

StreamingHashJoin::~StreamingHashJoin() noexcept
{
    LOG_INFO(
        log,
        "Left stream metrics: {}, Right stream metrics: {}, join metrics: {}",
        left_data->metrics.string(),
        right_data->metrics.string(),
        join_metrics.string());
}

StreamingHashJoin::Type StreamingHashJoin::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
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

void StreamingHashJoin::dataMapInit(MapsVariant & map)
{
    joinDispatchInit(kind, strictness, map);
    joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { map_.create(right_data->type); });
}

bool StreamingHashJoin::empty() const
{
    return right_data->type == Type::EMPTY;
}

bool StreamingHashJoin::alwaysReturnsEmptySet() const
{
    return false;
}

size_t StreamingHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    /// FIXME
    //    for (const auto & map : right_data->maps)
    //    {
    //        joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalRowCount(right_data->type); });
    //    }

    return res;
}

size_t StreamingHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    /// FIXME

    //    for (const auto & map : right_data->maps)
    //    {
    //        joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(right_data->type); });
    //    }
    //    res += right_data->pool.size();

    return res;
}

void StreamingHashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER StreamingHashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->forceHashJoin() || isRightOrFull(kind) || multiple_disjuncts;
    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (
        strictness == ASTTableJoin::Strictness::Range || strictness == ASTTableJoin::Strictness::RangeAsof
        || strictness == ASTTableJoin::Strictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
    {
        if (!saved_block_sample.findByName(column.name))
        {
            saved_block_sample.insert(column);
        }
    }

    if (nullable_right_side)
    {
        JoinCommon::convertColumnsToNullable(saved_block_sample, (isFull(kind) && !multiple_disjuncts ? right_table_keys.columns() : 0));
    }
}

Block StreamingHashJoin::structureRightBlock(const Block & block) const
{
    Block structured_block;

    structured_block.info = block.info;

    for (const auto & sample_column : savedBlockSample().getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);
        structured_block.insert(column);
    }

    return structured_block;
}

bool StreamingHashJoin::addJoinedBlock(const Block & source_block, bool check_limits)
{
    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right stream block for StreamingHashJoin: {}", source_block.rows());

    /// There's no optimization for right side const columns. Remove constness if any.
    Block materialized_block = materializeBlock(source_block);

    auto do_add_block = [this, check_limits](Int64 bucket, Block && block) {
        ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(block, table_join->getAllNames(JoinTableSide::Right));

        Block structured_block = structureRightBlock(block);

        size_t total_rows = 0;
        size_t total_bytes = 0;

        const auto & onexpr = table_join->getClauses().front();

        ColumnRawPtrs key_columns;
        for (const auto & name : onexpr.key_names_right)
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

        auto join_mask_col = JoinCommon::getColumnAsMask(block, onexpr.condColumnNames().second);
        /// Save blocks that do not hold conditions in ON section
        ColumnUInt8::MutablePtr not_joined_map = nullptr;
        if (isRightOrFull(kind) && !join_mask_col.isConstant())
        {
            const auto & join_mask = join_mask_col.getData();
            /// Save rows that do not hold conditions
            not_joined_map = ColumnUInt8::create(block.rows(), 0);
            for (size_t i = 0, sz = join_mask->size(); i < sz; ++i)
            {
                /// Condition hold, do not save row
                if ((*join_mask)[i])
                    continue;

                /// NULL key will be saved anyway because, do not save twice
                if (save_nullmap && (*null_map)[i])
                    continue;

                not_joined_map->getData()[i] = 1;
            }
        }

        right_data->insertBlock(
            bucket,
            std::move(structured_block),
            key_columns,
            join_mask_col,
            std::move(null_map),
            save_nullmap,
            std::move(null_map_holder),
            std::move(not_joined_map));

        if (!check_limits)
            return true;

        /// TODO: Do not calculate them every time
        total_rows = getTotalRowCount();
        total_bytes = getTotalByteCount();

        return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    };

    if (strictness == ASTTableJoin::Strictness::All)
    {
        return do_add_block(-1, std::move(materialized_block));
    }
    else
    {
        auto bucketed_blocks = right_data->range_splitter->split(std::move(materialized_block));

        for (auto & bucket_block : bucketed_blocks)
            if (!do_add_block(bucket_block.first, std::move(bucket_block.second)))
                return false;
    }

    return true;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void StreamingHashJoin::joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & onexprs = table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = onexprs[i].key_names_left;
        join_on_keys.emplace_back(block, key_names, onexprs[i].condColumnNames().first, key_sizes[i]);
    }
    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" stream must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    /*if constexpr (jf.right || jf.full)
    {
        materializeBlockInplace(block);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }*/

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns added_columns(
        block_with_columns_to_add,
        block,
        savedBlockSample(),
        *this,
        std::move(join_on_keys),
        &left_data->current_join_blocks->joined_rows,
        left_data->range_asof_join_ctx,
        jf.is_asof_join || jf.is_range_asof_join || jf.is_range_join);

    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_right_keys;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, right_data->type);

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            // renamed ???
            if (!block.findByName(right_key.name))
            {
                const auto & left_name = required_right_keys_sources[i];

                /// asof column is already in block.
                if ((jf.is_asof_join || jf.is_range_asof_join) && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = nullable_right_side || right_key.type->isNullable();
                auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
                ColumnWithTypeAndName right_col(col.column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = JoinStuff::correctNullability(std::move(right_col), is_nullable);
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
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            if (!block.findByName(right_col_name /*right_key.name*/))
            {
                const auto & left_name = required_right_keys_sources[i];

                /// asof column is already in block.
                if ((jf.is_asof_join || jf.is_range_asof_join) && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = nullable_right_side || right_key.type->isNullable();

                ColumnPtr thin_column = JoinStuff::filterWithBlanks(col.column, filter);

                ColumnWithTypeAndName right_col(thin_column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = JoinStuff::correctNullability(std::move(right_col), is_nullable, null_map_filter);
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

void StreamingHashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
}

void StreamingHashJoin::joinBlock(Block & block, ExtraBlockPtr & /*not_processed*/)
{
    assert(right_data->current_join_blocks && left_data->current_join_blocks);

    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block, onexpr.key_names_left, cond_column_name.first, right_sample_block, onexpr.key_names_right, cond_column_name.second);
    }

//    if (kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full)
//    {
//        materializeBlockInplace(block);
//        if (nullable_left_side)
//            JoinCommon::convertColumnsToNullable(block);
//    }

    std::vector<const std::decay_t<decltype(right_data->current_join_blocks->maps[0])> *> maps_vector;
    for (size_t i = 0; i < table_join->getClauses().size(); ++i)
        maps_vector.push_back(&right_data->current_join_blocks->maps[i]);

    if (joinDispatch(kind, strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_) {
            joinBlockImpl<kind_, strictness_>(block, sample_block_with_columns_to_add, maps_vector_);
        }))
    {
        /// Joined
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
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

std::shared_ptr<NotJoinedBlocks> StreamingHashJoin::getNonJoinedBlocks(
    const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    return {};
}

const ColumnWithTypeAndName & StreamingHashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable if nullable_right_side is true
    return savedBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

/// proton : starts
void StreamingHashJoin::initData()
{
    if (strictness == ASTTableJoin::Strictness::Range || strictness == ASTTableJoin::Strictness::RangeAsof)
    {
        const auto & left_asof_key_col = table_join->getOnlyClause().key_names_right.back();
        const auto & right_asof_key_col = table_join->getOnlyClause().key_names_left.back();

        const auto & range_join_ctx = table_join->rangeAsofJoinContext();

        right_data = std::make_shared<RightTableData>(range_join_ctx, left_asof_key_col, this);
        left_data = std::make_shared<LeftTableData>(range_join_ctx, right_asof_key_col, this);
    }
    else
    {
        right_data = std::make_shared<RightTableData>(this);
        left_data = std::make_shared<LeftTableData>(this);
    }
}

void StreamingHashJoin::initHashMaps(std::vector<MapsVariant> & all_maps)
{
    all_maps.resize(table_join->getClauses().size());
    for (auto & maps : all_maps)
        dataMapInit(maps);
}

Block StreamingHashJoin::joinBlocksAll(size_t & left_cached_bytes, size_t & right_cached_bytes)
{
    Block result;
    ExtraBlockPtr not_processed;

    std::scoped_lock lock(left_data->mutex, right_data->mutex);

    left_cached_bytes = left_data->metrics.total_bytes;
    right_cached_bytes = right_data->metrics.total_bytes;

    ++join_metrics.total_join;

    if (!left_data->hasNewData() && !right_data->hasNewData())
    {
        ++join_metrics.no_new_data_skip;
        return result;
    }

    if (right_data->current_join_blocks->blocks.empty() && kind != ASTTableJoin::Kind::Left)
        return result;

    auto left_block_start = left_data->current_join_blocks->blocks.begin();
    if (!right_data->hasNewData())
    {
        /// if left bucket has new data and right bucket doesn't have new data
        /// Only join new data from left bucket with right bucket data
        left_block_start = left_data->current_join_blocks->new_data_iter;
        ++join_metrics.only_join_new_data;
    }

    auto left_block_end = left_data->current_join_blocks->blocks.end();
    for (; left_block_start != left_block_end; ++left_block_start)
    {
        auto join_block{*left_block_start}; /// need a copy here since joinBlock will change the block passed-in in place

        joinBlock(join_block, not_processed);

        if (join_block.rows())
        {
            /// Has joined rows
            if (result)
            {
                assertBlocksHaveEqualStructure(result, join_block, "Hashed joined block");
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
    left_data->current_join_blocks->markNoNewData();
    left_data->setHasNewData(false);
    right_data->current_join_blocks->markNoNewData();
    right_data->setHasNewData(false);

    return result;
}

Block StreamingHashJoin::joinBlocks(size_t & left_cached_bytes, size_t & right_cached_bytes)
{
    if (strictness == ASTTableJoin::Strictness::All)
        return joinBlocksAll(left_cached_bytes, right_cached_bytes);

    Block result;
    ExtraBlockPtr not_processed;

    {
        std::scoped_lock lock(left_data->mutex, right_data->mutex);

        left_cached_bytes = left_data->metrics.total_bytes;
        right_cached_bytes = right_data->metrics.total_bytes;

        ++join_metrics.total_join;

        if (!left_data->hasNewData() && !right_data->hasNewData())
        {
            ++join_metrics.no_new_data_skip;
            return result;
        }

        for (auto & [left_bucket, left_bucket_blocks] : left_data->blocks)
        {
            auto right_iter = right_data->hashed_blocks.lower_bound(left_bucket - left_data->bucket_size * left_data->join_start_bucket);
            if (right_iter == right_data->hashed_blocks.end())
                /// no joined data
                continue;

            left_data->current_join_blocks = left_bucket_blocks;

            for (auto right_end = right_data->hashed_blocks.end(), right_begin = right_iter; right_begin != right_end; ++right_begin)
            {
                if (right_begin->first > left_bucket + left_data->bucket_size * left_data->join_stop_bucket)
                    /// Reaching the upper bound of right bucket to join
                    break;

                if (!left_bucket_blocks->hasNewData() && !right_begin->second->hasNewData())
                {
                    /// Ignore this bucket if there are no new data on both bucket since last join
                    ++join_metrics.time_bucket_no_new_data_skip;
                    continue;
                }

                /// Although we are bucketing blocks, but the real min/max in 2 buckets may not be join-able
                /// If [min, max] of left bucket doesn't intersect with right time bucket as a whole,
                /// we are sure there will be no join-able rows for the whole bucket
                if (!left_data->intersect(
                        left_bucket_blocks->min_ts, left_bucket_blocks->max_ts, right_begin->second->min_ts, right_begin->second->max_ts))
                {
                    ++join_metrics.time_bucket_no_intersection_skip;
                    continue;
                }

                /// Setup current_right_join_blocks which will be used by `joinBlock`
                right_data->current_join_blocks = right_begin->second;

                auto left_block_start = left_bucket_blocks->blocks.begin();
                if (left_bucket_blocks->hasNewData() && !right_data->current_join_blocks->hasNewData())
                {
                    /// if left bucket has new data and right bucket doesn't have new data
                    /// Only join new data from left bucket with right bucket data
                    left_block_start = left_bucket_blocks->new_data_iter;
                    ++join_metrics.only_join_new_data;
                }

                for (auto left_block_end = left_bucket_blocks->blocks.end(); left_block_start != left_block_end; ++left_block_start)
                {
                    /// If [min, max] of left block doesn't intersect with right time bucket
                    /// we are sure there will be no join-able rows
                    if (!left_data->intersect(
                            left_block_start->info.watermark_lower_bound,
                            left_block_start->info.watermark,
                            right_data->current_join_blocks->min_ts,
                            right_data->current_join_blocks->max_ts))
                    {
                        ++join_metrics.left_block_and_right_time_bucket_no_intersection_skip;
                        continue;
                    }

                    /// auto join_block{left_block_start->deepClone()};
                    auto join_block{*left_block_start}; /// need a copy here since joinBlock will change the block passed-in in place

                    joinBlock(join_block, not_processed);

                    if (join_block.rows())
                    {
                        /// Has joined rows
                        if (result)
                        {
                            assertBlocksHaveEqualStructure(result, join_block, "Hashed joined block");
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
                right_data->current_join_blocks->markNoNewData();
                left_bucket_blocks->markNoNewData();
            }
        }

        /// After full scan join, clear the new data flag. An optimization for periodical timeout which triggers join
        /// and there is no new data inserted
        left_data->setHasNewData(false);
        right_data->setHasNewData(false);
    }

    calculateWatermark();

    std::tie(left_cached_bytes, right_cached_bytes) = removeOldData();

    return result;
}

size_t StreamingHashJoin::insertLeftBlock(Block left_input_block)
{
    if (strictness == ASTTableJoin::Strictness::All)
        left_data->insertBlock(std::move(left_input_block));
    else
        left_data->insertBlockToTimeBucket(std::move(left_input_block));

    /// We don't hold the lock, it is ok to have stale numbers
    return left_data->metrics.total_bytes;
}

size_t StreamingHashJoin::insertRightBlock(Block right_input_block)
{
    addJoinedBlock(right_input_block, false);
    /// We don't hold the lock, it is ok to have stale numbers
    return right_data->metrics.total_bytes;
}

void StreamingHashJoin::calculateWatermark()
{
    Int64 left_watermark = left_data->current_watermark;
    Int64 right_watermark = right_data->current_watermark;

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

std::pair<size_t, size_t> StreamingHashJoin::removeOldData()
{
    return {left_data->removeOldData(), right_data->removeOldData()};
}

void StreamingHashJoin::LeftTableData::insertBlock(Block && block)
{
    assert(current_join_blocks);

    std::scoped_lock lock(mutex);
    block.info.setBlockId(block_id++);
    current_join_blocks->insertBlock(std::move(block));
    setHasNewData(true);
}

void StreamingHashJoin::LeftTableData::insertBlockToTimeBucket(Block && block)
{
    /// Categorize block according to time bucket, then we can prune the time bucketed blocks
    /// when `watermark` passed its time
    auto bucketed_blocks = range_splitter->split(std::move(block));

    std::vector<std::pair<UInt64, size_t>> late_blocks;

    {
        std::scoped_lock lock(mutex);

        for (auto & bucket_block : bucketed_blocks)
        {
            if (static_cast<Int64>(bucket_block.first) + bucket_size < join->combined_watermark)
            {
                late_blocks.emplace_back(bucket_block.first, bucket_block.second.rows());
                continue;
            }

            /// assign block ID
            bucket_block.second.info.setBlockId(block_id++);

            auto iter = blocks.find(bucket_block.first);
            if (iter != blocks.end())
            {
                iter->second->insertBlock(std::move(bucket_block.second));
            }
            else
            {
                blocks.emplace(bucket_block.first, std::make_shared<LeftTableBlocks>(std::move(bucket_block.second), this));

                /// Update watermark
                if (static_cast<Int64>(bucket_block.first) > current_watermark)
                    current_watermark = bucket_block.first;
            }

            setHasNewData(true);
        }
    }

    Int64 watermark = join->combined_watermark;
    for (auto [bucket, rows] : late_blocks)
    {
        LOG_INFO(
            join->log,
            "Discard {} late events in time bucket {} of left stream since it is later than latest combined watermark {} with "
            "bucket_size={}",
            rows,
            bucket,
            watermark,
            bucket_size);
    }
}

void StreamingHashJoin::RightTableData::insertBlock(
    Int64 bucket,
    Block block,
    ColumnRawPtrs & key_columns,
    JoinCommon::JoinMask & join_mask_col,
    ConstNullMapPtr null_map,
    UInt8 save_nullmap,
    ColumnPtr null_map_holder,
    ColumnUInt8::MutablePtr not_joined_map)
{
    assert(block);

    auto rows = block.rows();

    if (bucket > 0 && bucket + bucket_size < join->combined_watermark)
    {
        LOG_INFO(
            join->log,
            "Discard {} late events in time bucket {} of right stream since it is later than latest combined watermark {} with "
            "bucket_size={}",
            rows,
            bucket,
            join->combined_watermark.load(),
            bucket_size);
        return;
    }

    RightTableBlocks * data = nullptr;

    std::scoped_lock lock(mutex);

    if (bucket > 0)
    {
        auto iter = hashed_blocks.find(bucket);
        if (iter == hashed_blocks.end())
        {
            /// init this RightTableBlocks of this bucket
            std::tie(iter, std::ignore) = hashed_blocks.emplace(bucket, std::make_shared<RightTableBlocks>(this));
            join->initHashMaps(iter->second->maps);
        }

        /// Update watermark
        if (bucket > current_watermark)
            current_watermark = bucket;

        data = iter->second.get();
    }
    else
    {
        assert(current_join_blocks);
        data = current_join_blocks.get();
    }

    data->insertBlock(std::move(block));
    setHasNewData(true);

    Block * stored_block = &data->blocks.back();

    joinDispatch(join->kind, join->strictness, data->maps[0], [&](auto /*kind_*/, auto strictness_, auto & map) {
        [[maybe_unused]] size_t size = insertFromBlockImpl<strictness_>(
            *join,
            type,
            map,
            rows,
            key_columns,
            join->key_sizes[0],
            stored_block,
            null_map,
            /// If mask is false constant, rows are added to hashmap anyway. It's not a happy-flow, so this case is not optimized
            join_mask_col.getData(),
            data->pool);
    });

    if (save_nullmap)
        data->blocks_nullmaps.emplace_back(stored_block, null_map_holder);

    if (not_joined_map)
        data->blocks_nullmaps.emplace_back(stored_block, std::move(not_joined_map));
}
/// proton : ends
}
