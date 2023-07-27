#pragma once

#include <Core/Block.h> /// Block.h shall be included before RefCountBlockList since it depends on it for template initialization

#include <Columns/IColumn.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/RefCountBlockList.h>
#include <Interpreters/Streaming/joinSerder_fwd.h>
#include <Interpreters/Streaming/joinTuple.h>
#include <base/sort.h>
#include <Common/Arena.h>

#include <algorithm>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <variant>

namespace DB
{
namespace Streaming
{
/**
 * This class is intended to push sortable data into.
 * When looking up values the container ensures that it is sorted for log(N) lookup
 * The StreamingHashJoin will ensure the synchronization access to the data structure
 */

/// Reference to the row in block with reference count
template <typename DataBlock>
struct RowRefWithRefCount
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    RefCountBlockList<DataBlock> * blocks;
    typename RefCountBlockList<DataBlock>::iterator block_iter;
    SizeT row_num;

    /// We need this default ctor since hash table framework will call the default one initially
    RowRefWithRefCount() : blocks(nullptr), row_num(0) { }

    RowRefWithRefCount(RefCountBlockList<DataBlock> * blocks_, size_t row_num_)
        : blocks(blocks_), block_iter(blocks_->lastBlockIter()), row_num(static_cast<SizeT>(row_num_))
    {
        assert(blocks_);
    }

    RowRefWithRefCount(const RowRefWithRefCount & other) : blocks(other.blocks), block_iter(other.block_iter), row_num(other.row_num)
    {
        if (likely(blocks))
            block_iter->ref();
    }

    RowRefWithRefCount & operator=(const RowRefWithRefCount & other)
    {
        /// First deref existing block count
        if (this == &other)
            return *this;

        deref();

        blocks = other.blocks;
        block_iter = other.block_iter;
        row_num = other.row_num;

        if (likely(blocks))
            block_iter->ref();

        return *this;
    }

    /// If the current `block_iter` is the head of the block list and
    /// it is just last reference to that block, then deref it will cause
    /// removal of block list header
    bool willDerefRemoveListHeader() const
    {
        assert(blocks);
        return block_iter == blocks->begin() && block_iter->refCount() == 1;
    }

    Int64 blockID() const
    {
        assert(blocks);
        if constexpr (std::is_same_v<DataBlock, Block>)
            return block_iter->block.blockID();
        else
            return 0;
    }

    const DataBlock & block() const
    {
        assert(blocks);
        return block_iter->block;
    }

    ~RowRefWithRefCount() { deref(); }

    void serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(
        RefCountBlockList<DataBlock> * block_list,
        const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
        ReadBuffer & rb);

private:
    void ALWAYS_INLINE deref()
    {
        if (likely(blocks))
        {
            block_iter->deref();
            if (block_iter->refCount() == 0)
                blocks->erase(block_iter);
        }
    }
};

/// Use linked list to maintain the row refs
/// since we may delete some of the row refs when there is override
/// Used for partial primary key join scenarios
struct RowRefListMultiple
{
    using Iterator = typename std::list<RowRefWithRefCount<Block>>::iterator;

    std::list<RowRefWithRefCount<Block>> rows;

    Iterator insert(RefCountBlockList<Block> * blocks, size_t row_num) { return rows.emplace(rows.end(), blocks, row_num); }

    void erase(Iterator iterator) { rows.erase(iterator); }

    void serialize(
        const SerializedBlocksToIndices & serialized_blocks_to_indices,
        WriteBuffer & wb,
        SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices = nullptr) const;
    void deserialize(
        RefCountBlockList<Block> * block_list,
        const DeserializedIndicesToBlocks<Block> & deserialized_indices_to_blocks,
        ReadBuffer & rb,
        DeserializedIndicesToRowRefListMultiple * deserialized_indices_to_row_ref_list_multiple = nullptr);
};

using RowRefListMultiplePtr = std::unique_ptr<RowRefListMultiple>;

struct RowRefListMultipleRef
{
    RowRefListMultiple * row_ref_list = nullptr;
    RowRefListMultiple::Iterator iterator;

    void erase()
    {
        if (row_ref_list == nullptr)
            /// Already erased or empty state
            return;

        row_ref_list->erase(iterator);

        row_ref_list = nullptr;
        iterator = RowRefListMultiple::Iterator{};
    }

    void serialize(const SerializedRowRefListMultipleToIndices & serialized_row_ref_list_multiple_to_indices, WriteBuffer & wb) const;
    void deserialize(const DeserializedIndicesToRowRefListMultiple & deserialized_indices_to_row_ref_list_multiple, ReadBuffer & rb);
};

using RowRefListMultipleRefPtr = std::unique_ptr<RowRefListMultipleRef>;

template <typename TEntry>
class SortedLookupVector
{
public:
    using Base = std::vector<TEntry>;

    void insert(TEntry entry, bool ascending)
    {
        auto it = std::lower_bound(array.begin(), array.end(), entry, (ascending ? less : greater));
        array.insert(it, entry);
    }

    const RowRefWithRefCount<Block> * upperBound(const TEntry & k, bool ascending)
    {
        auto it = std::upper_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    const RowRefWithRefCount<Block> * lowerBound(const TEntry & k, bool ascending)
    {
        auto it = std::lower_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    void truncateTo(size_t max_size, bool ascending)
    {
        if (array.size() > max_size)
        {
            if (ascending)
                array.erase(array.begin(), array.begin() + array.size() - max_size);
            else
                array.erase(array.end() - (array.size() - max_size), array.end());

            array.shrink_to_fit();
        }
    }

    using iterator = typename Base::iterator;
    using const_iterator = typename Base::const_iterator;

    iterator begin() { return array.begin(); }
    iterator end() { return array.end(); }

    size_t size() const { return array.size(); }
    void resize(size_t s) { array.resize(s); }

    const_iterator begin() const { return array.begin(); }
    const_iterator end() const { return array.end(); }

private:
    Base array;

    static bool less(const TEntry & a, const TEntry & b) { return a.asof_value < b.asof_value; }

    static bool greater(const TEntry & a, const TEntry & b) { return a.asof_value > b.asof_value; }
};

class AsofRowRefs
{
public:
    template <typename T>
    struct Entry
    {
        using LookupType = SortedLookupVector<Entry<T>>;
        using LookupPtr = std::unique_ptr<LookupType>;
        T asof_value;
        RowRefWithRefCount<Block> row_ref;

        Entry() = default;
        Entry(T v) : asof_value(v) { }
        Entry(T v, RowRefWithRefCount<Block> rr) : asof_value(v), row_ref(rr) { }
    };

    using Lookups = std::variant<
        Entry<UInt8>::LookupPtr,
        Entry<UInt16>::LookupPtr,
        Entry<UInt32>::LookupPtr,
        Entry<UInt64>::LookupPtr,
        Entry<Int8>::LookupPtr,
        Entry<Int16>::LookupPtr,
        Entry<Int32>::LookupPtr,
        Entry<Int64>::LookupPtr,
        Entry<Float32>::LookupPtr,
        Entry<Float64>::LookupPtr,
        Entry<Decimal32>::LookupPtr,
        Entry<Decimal64>::LookupPtr,
        Entry<Decimal128>::LookupPtr,
        Entry<DateTime64>::LookupPtr>;

    AsofRowRefs() { }

    AsofRowRefs(TypeIndex t);

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    void insert(
        TypeIndex type,
        const IColumn & asof_column,
        RefCountBlockList<Block> * blocks,
        size_t row_num,
        ASOFJoinInequality inequality,
        size_t keep_versions);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    const RowRefWithRefCount<Block> *
    findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const;

    void serialize(TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(
        TypeIndex type,
        RefCountBlockList<Block> * block_list,
        const DeserializedIndicesToBlocks<Block> & deserialized_indices_to_blocks,
        ReadBuffer & rb);

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

class RangeAsofRowRefs
{
public:
    /// FIXME, multimap data structure
    template <typename KeyT>
    using LookupType = std::multimap<KeyT, RowRef>;

    template <typename KeyT>
    using LookupPtr = std::unique_ptr<LookupType<KeyT>>;

    using Lookups = std::variant<
        LookupPtr<UInt8>,
        LookupPtr<UInt16>,
        LookupPtr<UInt32>,
        LookupPtr<UInt64>,
        LookupPtr<Int8>,
        LookupPtr<Int16>,
        LookupPtr<Int32>,
        LookupPtr<Int64>,
        LookupPtr<Float32>,
        LookupPtr<Float64>,
        LookupPtr<Decimal32>,
        LookupPtr<Decimal64>,
        LookupPtr<Decimal128>,
        LookupPtr<DateTime64>>;

    RangeAsofRowRefs() { }

    explicit RangeAsofRowRefs(TypeIndex t);

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & size)
    {
        return AsofRowRefs::getTypeSize(asof_column, size);
    }

    void insert(TypeIndex type, const IColumn & asof_column, const Block * block, size_t row_num);

    /// Find a range of rows which can be joined
    std::vector<RowRef> findRange(
        TypeIndex type,
        const RangeAsofJoinContext & range_join_ctx,
        const IColumn & asof_column,
        size_t row_num,
        UInt64 src_block_id,
        bool is_left_block) const;

    /// Find the last one
    const RowRef *
    findAsof(TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num, UInt64 src_block_id)
        const;

    void serialize(TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(TypeIndex type, const DeserializedIndicesToBlocks<Block> & deserialized_indices_to_blocks, ReadBuffer & rb);

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};
}
}
