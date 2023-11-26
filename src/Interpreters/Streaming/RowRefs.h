#pragma once

#include <Core/Block.h> /// Block.h shall be included before RefCountBlockList since it depends on it for template initialization

#include <Columns/IColumn.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/RefCountDataBlockList.h>
#include <Interpreters/Streaming/SortedLookupContainer.h>
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
/// Reference to the row in block.
template <typename DataBlock>
struct RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const DataBlock * block = nullptr;
    SizeT row_num = 0;

    RowRef() = default;
    RowRef(const DataBlock * block_, size_t row_num_) : block(block_), row_num(static_cast<SizeT>(row_num_)) { }

    void serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb);
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
template <typename DataBlock>
struct RowRefList : RowRef<DataBlock>
{
    using RowRefDataBlock = RowRef<DataBlock>;
    using SizeT = typename RowRefDataBlock::SizeT;
    /// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
    struct Batch
    {
        static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

        SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.
        Batch * next;
        RowRefDataBlock row_refs[MAX_SIZE];

        explicit Batch(Batch * parent) : next(parent) { }

        bool full() const { return size == MAX_SIZE; }

        Batch * insert(RowRefDataBlock && row_ref, Arena & pool)
        {
            if (full())
            {
                auto * batch = pool.alloc<Batch>();
                *batch = Batch(this);
                batch->insert(std::move(row_ref), pool);
                return batch;
            }

            row_refs[size++] = std::move(row_ref);
            return this;
        }
    };

    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const RowRefList * begin) : root(begin), first(true), batch(root->next), position(0) { }

        const RowRefDataBlock * operator->() const
        {
            if (first)
                return root;
            return &batch->row_refs[position];
        }

        const RowRefDataBlock * operator*() const
        {
            if (first)
                return root;
            return &batch->row_refs[position];
        }

        void operator++()
        {
            if (first)
            {
                first = false;
                return;
            }

            if (batch)
            {
                ++position;
                if (position >= batch->size)
                {
                    batch = batch->next;
                    position = 0;
                }
            }
        }

        bool ok() const { return first || batch; }

    private:
        const RowRefList * root;
        bool first;
        Batch * batch;
        size_t position;
    };

    RowRefList() { } /// NOLINT
    RowRefList(const DataBlock * block_, size_t row_num_) : RowRefDataBlock(block_, row_num_) { }

    ForwardIterator begin() const { return ForwardIterator(this); }

    /// insert element after current one
    void insert(RowRefDataBlock && row_ref, Arena & pool)
    {
        if (!next)
        {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

    void serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(Arena & pool, const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb);

private:
    Batch * next = nullptr;
};

/// Reference to the row in block with reference count
template <typename DataBlock>
struct RowRefWithRefCount
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    RefCountDataBlockList<DataBlock> * blocks;
    typename RefCountDataBlockList<DataBlock>::iterator block_iter;
    SizeT row_num;

    /// We need this default ctor since hash table framework will call the default one initially
    RowRefWithRefCount() : blocks(nullptr), row_num(0) { }

    RowRefWithRefCount(RefCountDataBlockList<DataBlock> * blocks_, size_t row_num_)
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

    const DataBlock & block() const
    {
        assert(blocks);
        return block_iter->block;
    }

    ~RowRefWithRefCount() { deref(); }

    void serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(
        RefCountDataBlockList<DataBlock> * block_list,
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
template <typename DataBlock>
struct RowRefListMultiple
{
    using RowRefDataBlock = RowRefWithRefCount<DataBlock>;
    using Iterator = typename std::list<RowRefDataBlock>::iterator;

    std::list<RowRefDataBlock> rows;

    Iterator insert(RefCountDataBlockList<DataBlock> * blocks, size_t row_num) { return rows.emplace(rows.end(), blocks, row_num); }

    void erase(Iterator iterator) { rows.erase(iterator); }

    void serialize(
        const SerializedBlocksToIndices & serialized_blocks_to_indices,
        WriteBuffer & wb,
        SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices = nullptr) const;
    void deserialize(
        RefCountDataBlockList<DataBlock> * block_list,
        const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
        ReadBuffer & rb,
        DeserializedIndicesToRowRefListMultiple<DataBlock> * deserialized_indices_to_row_ref_list_multiple = nullptr);
};

template <typename DataBlock>
using RowRefListMultiplePtr = std::unique_ptr<RowRefListMultiple<DataBlock>>;

template <typename DataBlock>
struct RowRefListMultipleRef
{
    RowRefListMultiple<DataBlock> * row_ref_list = nullptr;
    typename RowRefListMultiple<DataBlock>::Iterator iterator;

    void erase()
    {
        if (row_ref_list == nullptr)
            /// Already erased or empty state
            return;

        row_ref_list->erase(iterator);

        row_ref_list = nullptr;
        iterator = typename RowRefListMultiple<DataBlock>::Iterator{};
    }

    void serialize(const SerializedRowRefListMultipleToIndices & serialized_row_ref_list_multiple_to_indices, WriteBuffer & wb) const;
    void
    deserialize(const DeserializedIndicesToRowRefListMultiple<DataBlock> & deserialized_indices_to_row_ref_list_multiple, ReadBuffer & rb);
};

template <typename DataBlock>
using RowRefListMultipleRefPtr = std::unique_ptr<RowRefListMultipleRef<DataBlock>>;

template <typename DataBlock>
class AsofRowRefs
{
public:
    using RowRefDataBlock = RowRefWithRefCount<DataBlock>;
    template <typename T>
    struct Entry
    {
        using LookupType = SortedLookupContainer<RowRefDataBlock, Entry<T>>;
        using LookupPtr = std::unique_ptr<LookupType>;

        T asof_value;
        RowRefDataBlock row_ref;

        Entry() = default;
        Entry(T v) : asof_value(v) { }
        Entry(T v, RowRefDataBlock rr) : asof_value(v), row_ref(rr) { }
    };

    using Lookups = std::variant<
        typename Entry<UInt8>::LookupPtr,
        typename Entry<UInt16>::LookupPtr,
        typename Entry<UInt32>::LookupPtr,
        typename Entry<UInt64>::LookupPtr,
        typename Entry<Int8>::LookupPtr,
        typename Entry<Int16>::LookupPtr,
        typename Entry<Int32>::LookupPtr,
        typename Entry<Int64>::LookupPtr,
        typename Entry<Float32>::LookupPtr,
        typename Entry<Float64>::LookupPtr,
        typename Entry<Decimal32>::LookupPtr,
        typename Entry<Decimal64>::LookupPtr,
        typename Entry<Decimal128>::LookupPtr,
        typename Entry<DateTime64>::LookupPtr>;

    AsofRowRefs() { }

    AsofRowRefs(TypeIndex t);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    void insert(
        TypeIndex type,
        const IColumn & asof_column,
        RefCountDataBlockList<DataBlock> * blocks,
        size_t row_num,
        ASOFJoinInequality inequality,
        size_t keep_versions);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    const RowRefDataBlock * findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const;

    void serialize(TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(
        TypeIndex type,
        RefCountDataBlockList<DataBlock> * block_list,
        const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
        ReadBuffer & rb);

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

std::optional<TypeIndex> getAsofTypeSize(const IColumn & asof_column, size_t & type_size);

template <typename DataBlock>
class RangeAsofRowRefs
{
public:
    using RowRefDataBlock = RowRef<DataBlock>;
    /// FIXME, multimap data structure
    template <typename KeyT>
    using LookupType = std::multimap<KeyT, RowRefDataBlock>;

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

    void insert(TypeIndex type, const IColumn & asof_column, const DataBlock * block, size_t row_num);

    /// Find a range of rows which can be joined
    std::vector<RowRefDataBlock> findRange(
        TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num, bool is_left_block) const;

    /// Find the last one
    const RowRefDataBlock *
    findAsof(TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num) const;

    void serialize(TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const;
    void deserialize(TypeIndex type, const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb);

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};
}
}
