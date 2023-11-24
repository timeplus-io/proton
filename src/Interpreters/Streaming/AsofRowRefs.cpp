#include <Interpreters/Streaming/AsofRowRefs.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/LightChunk.h>

namespace DB::Streaming
{
namespace
{
/// maps enum values to types
template <typename F>
void callWithType(TypeIndex which, F && f)
{
    switch (which)
    {
        case TypeIndex::UInt8:
            return f(UInt8());
        case TypeIndex::UInt16:
            return f(UInt16());
        case TypeIndex::UInt32:
            return f(UInt32());
        case TypeIndex::UInt64:
            return f(UInt64());
        case TypeIndex::Int8:
            return f(Int8());
        case TypeIndex::Int16:
            return f(Int16());
        case TypeIndex::Int32:
            return f(Int32());
        case TypeIndex::Int64:
            return f(Int64());
        case TypeIndex::Float32:
            return f(Float32());
        case TypeIndex::Float64:
            return f(Float64());
        case TypeIndex::Decimal32:
            return f(Decimal32());
        case TypeIndex::Decimal64:
            return f(Decimal64());
        case TypeIndex::Decimal128:
            return f(Decimal128());
        case TypeIndex::DateTime64:
            return f(DateTime64());
        default:
            break;
    }

    UNREACHABLE();
}
}

template <typename DataBlock>
PagedAsofRowRefs<DataBlock>::PagedAsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;
        lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

template <typename DataBlock>
void PagedAsofRowRefs<DataBlock>::insert(
    TypeIndex type,
    const IColumn & asof_column,
    RefCountDataBlockPages<DataBlock> * blocks,
    size_t row_num,
    ASOFJoinInequality inequality,
    size_t keep_versions)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupPtr = typename Entry<T>::LookupPtr;

        auto & container = std::get<LookupPtr>(lookups);

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);
        bool ascending = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::LessOrEquals);
        container->insert(Entry<T>(key, RowRefDataBlock(blocks, row_num)), ascending, keep_versions);
    };

    callWithType(type, call);
}

template <typename DataBlock>
const typename PagedAsofRowRefs<DataBlock>::RowRefDataBlock *
PagedAsofRowRefs<DataBlock>::findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const
{
    const RowRefDataBlock * out = nullptr;

    bool ascending = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::LessOrEquals);
    bool is_strict = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::Greater);

    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using EntryType = Entry<T>;
        using LookupPtr = typename EntryType::LookupPtr;

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);
        T key = column.getElement(row_num);
        auto & typed_lookup = std::get<LookupPtr>(lookups);

        if (is_strict)
            out = typed_lookup->upperBound(EntryType(key), ascending);
        else
            out = typed_lookup->lowerBound(EntryType(key), ascending);
    };

    callWithType(type, call);
    return out;
}

template class PagedAsofRowRefs<LightChunk>;
}
