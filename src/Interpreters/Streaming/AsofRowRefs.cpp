#include <Interpreters/Streaming/AsofRowRefs.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/IColumn.h>

namespace DB::Streaming
{
template <typename DataBlock>
void PagedAsofRowRefs<DataBlock>::insert(
    TypeIndex type,
    const IColumn & asof_column,
    PageBasedRowRefWithRefCount<DataBlock> * blocks,
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
        container->insert(Entry<T>(key, RowRefDataBlock(blocks, row_num)), ascending);
        container->truncateTo(keep_versions, ascending);
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

}
