#pragma once

#include "JoinStreamDescription.h"
#include "RowRefs.h"
#include "joinData.h"
#include "joinKind.h"

#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/ColumnUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace Streaming
{
/** Data structure for implementation of Streaming JOIN which is always kind of ASOF join since its last join key
  * is always inequality of timestamps. This implementation is a copy of existing HashJoin and adapt to streaming scenario.
  * This implementation is a copy of existing HashJoin and adapted to streaming scenario. The following comments are copied
  * as well which shall be revised accordingly
  *
  * It is just a hash table: keys -> rows of joined ("right") table.
  * Additionally, CROSS JOIN is supported: instead of hash table, it use just set of blocks without keys.
  *
  * JOIN-s could be only
  * - ASOF x LEFT/INNER
  *
  * ALL means usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY uses one line per unique key from right table. For LEFT JOIN it would be any row (with needed joined key) from the right table,
  * for RIGHT JOIN it would be any row from the left table and for INNER one it would be any row from right and any row from left.
  * SEMI JOIN filter left table by keys that are present in right table for LEFT JOIN, and filter right table by keys from left table
  * for RIGHT JOIN. In other words SEMI JOIN returns only rows which joining keys present in another table.
  * ANTI JOIN is the same as SEMI JOIN but returns rows with joining keys that are NOT present in another table.
  * SEMI/ANTI JOINs allow to get values from both tables. For filter table it gets any row with joining same key. For ANTI JOIN it returns
  * defaults other table columns.
  * ASOF JOIN is not-equi join. For one key column it finds nearest value to join according to join inequality.
  * It's expected that ANY|SEMI LEFT JOIN is more efficient that ALL one.
  *
  * If INNER is specified - leave only rows that have matching rows from "right" table.
  * If LEFT is specified - in case when there is no matching row in "right" table, fill it with default values instead.
  * If RIGHT is specified - first process as INNER, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  * If FULL is specified - first process as LEFT, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  *
  * Thus, LEFT and RIGHT JOINs are not symmetric in terms of implementation.
  *
  * All JOINs (except CROSS) are done by equality condition on keys (equijoin).
  * Non-equality and other conditions are not supported.
  *
  * Implementation:
  *
  * 1. Build hash table in memory from "right" table.
  * This hash table is in form of keys -> row in case of ANY or keys -> [rows...] in case of ALL.
  * This is done in insertFromBlock method.
  *
  * 2. Process "left" table and join corresponding rows from "right" table by lookups in the map.
  * This is done in joinBlock methods.
  *
  * In case of ANY LEFT JOIN - form new columns with found values or default values.
  * This is the most simple. Number of rows in left table does not change.
  *
  * In case of ANY INNER JOIN - form new columns with found values,
  *  and also build a filter - in what rows nothing was found.
  * Then filter columns of "left" table.
  *
  * In case of ALL ... JOIN - form new columns with all found rows,
  *  and also fill 'offsets' array, describing how many times we need to replicate values of "left" table.
  * Then replicate columns of "left" table.
  *
  * How Nullable keys are processed:
  *
  * NULLs never join to anything, even to each other.
  * During building of map, we just skip keys with NULL value of any component.
  * During joining, we simply treat rows with any NULLs in key as non joined.
  *
  * Default values for outer joins (LEFT, RIGHT, FULL):
  *
  * Behaviour is controlled by 'join_use_nulls' settings.
  * If it is false, we substitute (global) default value for the data type, for non-joined rows
  *  (zero, empty string, etc. and NULL for Nullable data types).
  * If it is true, we always generate Nullable column and substitute NULLs for non-joined rows,
  *  as in standard SQL.
  */
class HashJoin : public IJoin
{
public:
    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescription left_join_stream_desc_,
        JoinStreamDescription right_join_stream_desc_);

    ~HashJoin() noexcept override;

    /// proton : starts
    /// returns total bytes in left stream cache
    size_t insertLeftBlock(Block left_input_block);

    /// returns total bytes in right stream cache
    size_t insertRightBlock(Block right_input_block);

    Block joinBlocks(size_t & left_cached_bytes, size_t & right_cached_bytes);

    bool needBufferLeftStream() const { return right_stream_desc.hash_semantic != HashSemantic::VersionedKV; }

    bool isRightStreamDataRefCounted() const
    {
        return right_stream_desc.hash_semantic == HashSemantic::VersionedKV || right_stream_desc.hash_semantic == HashSemantic::ChangeLogKV;
    }

    UInt64 keepVersions() const { return right_stream_desc.keep_versions; }

    /// proton : ends

    const TableJoin & getTableJoin() const override { return *table_join; }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addJoinedBlock(const Block & block, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override;

    /** Join data from the map (that was previously built by calls to addJoinedBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    /** Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    void setTotals(const Block & block) override { totals = block; }
    const Block & getTotals() const override { return totals; }

    bool isFilled() const override { return false; }

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const final;

    bool alwaysReturnsEmptySet() const final;

    ASTTableJoin::Kind getKind() const { return kind; }
    ASTTableJoin::Strictness getStrictness() const { return strictness; }
    Strictness getStreamingStrictness() const { return streaming_strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOF::Inequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

/// proton : starts. copied from from HashJoin

/// Different types of keys for maps.
#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys128) \
    M(keys256) \
    M(hashed)


/// Used for reading from StorageJoin and applying joinGet function
#define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string)

    enum class Type
    {
        EMPTY,
        CROSS,
        DICT,
#define M(NAME) NAME,
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    };

    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        using MappedType = Mapped;
        std::unique_ptr<FixedHashMap<UInt8, Mapped>> key8;
        std::unique_ptr<FixedHashMap<UInt16, Mapped>> key16;
        std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>> key32;
        std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>> key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_fixed_string;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>> keys128;
        std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>> keys256;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>> hashed;

        void create(Type which)
        {
            switch (which)
            {
                case Type::EMPTY:
                    break;
                case Type::CROSS:
                    break;
                case Type::DICT:
                    break;

#define M(NAME) \
    case Type::NAME: \
        NAME = std::make_unique<typename decltype(NAME)::element_type>(); \
        break;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        size_t getTotalRowCount(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->size() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            __builtin_unreachable();
        }

        size_t getTotalByteCountImpl(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->getBufferSizeInBytes() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            __builtin_unreachable();
        }

        size_t getBufferSizeInCells(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            __builtin_unreachable();
        }
    };

    using MapsOne = MapsTemplate<RowRefWithRefCount>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;
    using MapsRangeAsof = MapsTemplate<RangeAsofRowRefs>;
    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsRangeAsof>;
    /// proton:ends

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

    void setAsofJoinColumnPositionAndScale(UInt16 scale, size_t left_asof_col_pos, size_t right_asof_col_pos, TypeIndex type_index)
    {
        left_data->updateAsofJoinColumnPositionAndScale(scale, left_asof_col_pos, type_index);
        right_data->updateAsofJoinColumnPositionAndScale(scale, right_asof_col_pos, type_index);
    }

    friend struct StreamData;
    friend struct LeftStreamData;
    friend struct RightStreamData;

private:
    void dataMapInit(MapsVariant &);

    const Block & savedBlockSample() const { return right_data->sample_block; }

    /// Modify (structure) right block to save it in block list
    Block structureRightBlock(const Block & stored_block) const;
    void initRightBlockStructure(Block & saved_block_sample);

    template <Kind KIND, Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const;

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    bool empty() const;

    /// proton : starts
    void initData();

    void initHashMaps(std::vector<MapsVariant> & all_maps);

    /// When left stream joins right stream, watermark calculation is more complicated.
    /// Firstly, each stream has its own watermark and progresses separately.
    /// Secondly, `combined_watermark` is calculated periodically according the watermarks in the left and right streams
    void calculateWatermark();

    std::pair<size_t, size_t> removeOldData();

    /// For global join without time bucket
    Block joinBlocksAll(size_t & left_cached_bytes, size_t & right_cached_bytes);
    void doJoinBlock(Block & block);
    /// proton : ends

private:
    template <bool>
    friend class NotJoinedHash;

    friend class JoinSource;

    std::shared_ptr<TableJoin> table_join;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    Kind streaming_kind;
    Strictness streaming_strictness;

    bool nullable_right_side; /// In case of LEFT and FULL joins, if use_nulls, convert right-side columns to Nullable.
    bool nullable_left_side; /// In case of RIGHT and FULL joins, if use_nulls, convert left-side columns to Nullable.
    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    std::optional<TypeIndex> asof_type;
    ASOF::Inequality asof_inequality;

    JoinStreamDescription left_stream_desc;
    JoinStreamDescription right_stream_desc;

    LeftStreamDataPtr left_data;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    /// mutable JoinStuff::JoinUsedFlags used_flags;
    RightStreamDataPtr right_data;
    Type right_hash_method_type;

    std::vector<Sizes> key_sizes;

    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table (duplicates appear once).
    Block right_table_keys;
    /// Block with key columns right-side table keys that are needed in result (would be attached after joined columns).
    Block required_right_keys;
    /// Left table column names that are sources for required_right_keys columns
    std::vector<String> required_right_keys_sources;

    Block totals;

    std::atomic_int64_t combined_watermark = 0;

    JoinGlobalMetrics join_metrics;

    Poco::Logger * log;
};

struct HashJoinMapsVariants
{
    std::vector<HashJoin::MapsVariant> map_variants;
};

}
}
