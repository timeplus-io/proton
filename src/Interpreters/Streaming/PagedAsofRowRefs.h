#pragma once

#include <Interpreters/Streaming/PageBasedRowRefWithRefCount.h>
#include <Interpreters/Streaming/SortedLookupContainer.h>

#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <Core/Types.h>

namespace DB::Streaming
{
template <typename DataBlock>
class PagedAsofRowRefs
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
        Entry(T v, RowRefDataBlock && row_ref_) : asof_value(v), row_ref(std::move(row_ref_)) { }
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

    PagedAsofRowRefs() { }

    PagedAsofRowRefs(TypeIndex t);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    void insert(
        TypeIndex type,
        const IColumn & asof_column,
        RefCountDataBlockPages<DataBlock> * blocks,
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
