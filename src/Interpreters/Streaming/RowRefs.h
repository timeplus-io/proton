#pragma once

#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/joinBlockList.h>
#include <Interpreters/Streaming/joinTuple.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/RowRefs.h>
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
struct RowRefWithRefCount
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    JoinBlockList * blocks;
    JoinBlockList::iterator block_iter;
    SizeT row_num;

    /// We need this default ctor since hash table framework will call the default one initially
    RowRefWithRefCount() : blocks(nullptr), row_num(0) { }

    RowRefWithRefCount(JoinBlockList * blocks_, size_t row_num_)
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
        return block_iter->block.info.blockID();
    }

    ~RowRefWithRefCount() { deref(); }

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

    const RowRefWithRefCount * upperBound(const TEntry & k, bool ascending)
    {
        auto it = std::upper_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    const RowRefWithRefCount * lowerBound(const TEntry & k, bool ascending)
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
        RowRefWithRefCount row_ref;

        Entry(T v) : asof_value(v) { }
        Entry(T v, RowRefWithRefCount rr) : asof_value(v), row_ref(rr) { }
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
        JoinBlockList * blocks,
        size_t row_num,
        ASOFJoinInequality inequality,
        size_t keep_versions);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    const RowRefWithRefCount * findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const;

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
    const RowRef * findAsof(
        TypeIndex type,
        const RangeAsofJoinContext & range_join_ctx,
        const IColumn & asof_column,
        size_t row_num,
        UInt64 src_block_id) const;

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};
}
}
