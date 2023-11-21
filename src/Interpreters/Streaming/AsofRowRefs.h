#pragma once

#include <Interpreters/Streaming/RefCountDataBlockPages.h>

#include <Core/Joins.h>
#include <Core/Types.h>

namespace DB::Streaming
{
/// Reference to the row in block with reference count
template <typename DataBlock>
struct PageBasedRowRefWithRefCount
{
    /// Which page the the current DataBlock resides in
    RefCountDataBlockPage<DataBlock> * page = nullptr;
    /// Offset inside the target page
    uint32_t page_offset = 0;
    /// Row number of the DataBlock
    uint32_t row_num = 0;

    /// We need this default ctor since hash table framework will call the default one initially
    PageBasedRowRefWithRefCount() = default;

    PageBasedRowRefWithRefCount(RefCountDataBlockPages<DataBlock> * block_pages, size_t row_num_)
        : page(block_pages->lastPage()), page_offset(block_pages->lastPageOffset()), row_num(static_cast<uint32_t>(row_num_))
    {
        assert(page);
    }

    PageBasedRowRefWithRefCount(const PageBasedRowRefWithRefCount & other)
        : page(other.page), page_offset(other.page_offset), row_num(other.row_num)
    {
        if (likely(page))
            page->ref(page_offset);
    }

    PageBasedRowRefWithRefCount & operator=(const PageBasedRowRefWithRefCount & other)
    {
        if (this == &other)
            return *this;

        /// First deref existing block count
        /// After deref, page may be GCed
        if (likely(page))
            page->deref(page_offset);

        page = other.page;
        page_offset = other.page_offset;
        row_num = other.row_num;

        if (likely(page))
            page->ref(page_offset);

        return *this;
    }

    const DataBlock & block() const
    {
        assert(page);
        return page->getDataBlock(page_offset);
    }

    ~PageBasedRowRefWithRefCount()
    {
        if (likely(page))
            page->deref(page_offset);
    }
};

template <typename RowRefDataBlock, typename TEntry>
class SortedLookupContainer
{
public:
    using Base = std::deque<TEntry>;

    void insert(TEntry entry, bool ascending, size_t max_size)
    {
        /// Happy and perf path
        if (ascending)
        {
            if (array.empty() || less(array.back(), entry)) /// Very likely
            {
                array.push_back(std::move(entry));
                if (array.size() > max_size)
                    array.pop_front();

                return;
            }
        }
        else
        {
            if (array.empty() || less(array.front(), entry)) /// likely
            {
                array.push_front(std::move(entry));
                if (array.size() > max_size)
                    array.pop_back();

                return;
            }
        }

        /// Slow path
        auto it = std::lower_bound(array.begin(), array.end(), entry, (ascending ? less : greater));
        array.insert(it, entry);
    }

    const RowRefDataBlock * upperBound(const TEntry & k, bool ascending)
    {
        auto it = std::upper_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    const RowRefDataBlock * lowerBound(const TEntry & k, bool ascending)
    {
        auto it = std::lower_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
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

template <typename DataBlock>
class AsofRowRefs
{
public:
    using RowRefDataBlock = PageBasedRowRefWithRefCount<DataBlock>;
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
        PageBasedRowRefWithRefCount<DataBlock> * blocks,
        size_t row_num,
        ASOFJoinInequality inequality,
        size_t keep_versions);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    const RowRefDataBlock * findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const;

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

}
