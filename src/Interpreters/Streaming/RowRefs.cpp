#include  <Interpreters/Streaming/RowRefs.h>

#include <base/types.h>
#include <Common/typeid_cast.h>
#include <Common/ColumnsHashing.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnDecimal.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_TYPE_OF_FIELD;
}

namespace Streaming
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

AsofRowRefs::AsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;
        lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

void AsofRowRefs::insert(
    TypeIndex type,
    const IColumn & asof_column,
    JoinBlockList * blocks,
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
        container->insert(Entry<T>(key, RowRefWithRefCount(blocks, row_num)), ascending);
        container->truncateTo(keep_versions, ascending);
    };

    callWithType(type, call);
}

const RowRefWithRefCount *
AsofRowRefs::findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const
{
    const RowRefWithRefCount * out = nullptr;

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

std::optional<TypeIndex> AsofRowRefs::getTypeSize(const IColumn & asof_column, size_t & size)
{
    TypeIndex idx = asof_column.getDataType();

    switch (idx)
    {
        case TypeIndex::UInt8:
            size = sizeof(UInt8);
            return idx;
        case TypeIndex::UInt16:
            size = sizeof(UInt16);
            return idx;
        case TypeIndex::UInt32:
            size = sizeof(UInt32);
            return idx;
        case TypeIndex::UInt64:
            size = sizeof(UInt64);
            return idx;
        case TypeIndex::Int8:
            size = sizeof(Int8);
            return idx;
        case TypeIndex::Int16:
            size = sizeof(Int16);
            return idx;
        case TypeIndex::Int32:
            size = sizeof(Int32);
            return idx;
        case TypeIndex::Int64:
            size = sizeof(Int64);
            return idx;
        //case TypeIndex::Int128:
        case TypeIndex::Float32:
            size = sizeof(Float32);
            return idx;
        case TypeIndex::Float64:
            size = sizeof(Float64);
            return idx;
        case TypeIndex::Decimal32:
            size = sizeof(Decimal32);
            return idx;
        case TypeIndex::Decimal64:
            size = sizeof(Decimal64);
            return idx;
        case TypeIndex::Decimal128:
            size = sizeof(Decimal128);
            return idx;
        case TypeIndex::DateTime64:
            size = sizeof(DateTime64);
            return idx;
        default:
            break;
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "ASOF join not supported for type: {}", asof_column.getFamilyName());
}

RangeAsofRowRefs::RangeAsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        lookups = std::make_unique<LookupType<T>>();
    };

    callWithType(type, call);
}

void RangeAsofRowRefs::insert(TypeIndex type, const IColumn & asof_column, const Block * block, size_t row_num)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);
        std::get<LookupPtr<T>>(lookups)->emplace(key, RowRef(block, row_num));
    };

    callWithType(type, call);
}

std::vector<RowRef> RangeAsofRowRefs::findRange(
    TypeIndex type,
    const RangeAsofJoinContext & range_join_ctx,
    const IColumn & asof_column,
    size_t row_num,
    UInt64 src_block_id,
    JoinTupleMap * joined_rows,
    bool is_left_block) const
{
    std::vector<RowRef> results;

    auto call_for_left_block = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] key - right_key [right_inequality] upper_bound
        /// Example: lower_bound < key - right_key <= upper_bound
        /// => -upper_bound <= right_key - key < -lower_bound
        /// => key - upper_bound <= right_key < key - lower_bound
        /// Find key range : [key - upper_bound, key - lower_bound)

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        if constexpr (is_decimal<T>)
            key -= static_cast<typename T::NativeType>(range_join_ctx.upper_bound);
        else
            key -= static_cast<T>(range_join_ctx.upper_bound);

        decltype(m->begin()) lower_iter;
        if (is_right_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if constexpr (is_decimal<T>)
        {
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); /// restore
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound); /// upper bound
        }
        else
        {
            key += static_cast<T>(range_join_ctx.upper_bound); /// restore
            key -= static_cast<T>(range_join_ctx.lower_bound); /// upper bound
        }

        if (lower_iter == m->end() || lower_iter->first > key)
            /// all keys in the map < key - upper_bound or
            /// all keys in the map > key - lower_bound
            return;

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;

        /// >= key
        auto upper_iter = m->lower_bound(key);

        if (is_left_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_left_strict || upper_iter->first > key)
            /// We need back one step in these cases
            --upper_iter;

        assert(upper_iter->first >= lower_iter->first);

        do
        {
            /// Add to results only if the right rows are not joined with the source rows in the src block
            if (!joined_rows
                || !joined_rows->contains(
                    JoinTuple{src_block_id, lower_iter->second.block, static_cast<uint32_t>(row_num), lower_iter->second.row_num}))
                results.push_back(lower_iter->second);
        } while (lower_iter++ != upper_iter); /// We need include value at upper_iter, so postfix lower_iter++
    };

    auto call_for_right_block = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] left_key - key [right_inequality] upper_bound
        /// Example: lower_bound < left_key - key <= upper_bound
        /// key + lower_bound < left_key <= key + upper_bound
        /// Find key range : [key + lower_bound, key + upper_bound)

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;

        if constexpr (is_decimal<T>)
            key += static_cast<typename T::NativeType>(range_join_ctx.lower_bound);
        else
            key += static_cast<T>(range_join_ctx.lower_bound);

        decltype(m->begin()) lower_iter;
        if (is_left_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if constexpr (is_decimal<T>)
        {
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound); /// restore
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); /// upper bound
        }
        else
        {
            key -= static_cast<T>(range_join_ctx.lower_bound); /// restore
            key += static_cast<T>(range_join_ctx.upper_bound); /// upper bound
        }

        if (lower_iter == m->end() || lower_iter->first > key)
            /// all keys in the map < key + lower_bound or
            /// all keys in the map > key + upper_bound
            return;

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        /// >= key
        auto upper_iter = m->lower_bound(key);

        if (is_right_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_right_strict || upper_iter->first > key)
            /// We need back one step in these cases
            --upper_iter;

        assert(upper_iter->first >= lower_iter->first);

        do
        {
            /// Add to results only if the left rows are not joined with the source rows in the src block
            if (!joined_rows
                || !joined_rows->contains(
                    JoinTuple{src_block_id, lower_iter->second.block, static_cast<uint32_t>(row_num), lower_iter->second.row_num}))
                results.push_back(lower_iter->second);
        } while (lower_iter++ != upper_iter); /// We need include value at upper_iter, so postfix lower_iter++
    };

    if (is_left_block)
        callWithType(type, call_for_left_block);
    else
        callWithType(type, call_for_right_block);
    return results;
}

const RowRef * RangeAsofRowRefs::findAsof(
    TypeIndex type,
    const RangeAsofJoinContext & range_join_ctx,
    const IColumn & asof_column,
    size_t row_num,
    UInt64 src_block_id,
    JoinTupleMap * joined_rows) const
{
    RowRef * result = nullptr;

    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] key - right_key [right_inequality] upper_bound
        /// Example: lower_bound < key - right_key <= upper_bound
        /// => -upper_bound <= right_key - key < -lower_bound
        /// => key - upper_bound <= right_key < key - lower_bound
        /// Find key range : [key - upper_bound, key - lower_bound)

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        if constexpr (is_decimal<T>)
            key -= static_cast<typename T::NativeType>(range_join_ctx.upper_bound);
        else
            key -= static_cast<T>(range_join_ctx.upper_bound);

        decltype(m->begin()) lower_iter;
        if (is_right_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if (lower_iter == m->end())
            /// all keys in the map < key - upper_bound
            return;

        assert(lower_iter->first >= key);

        if constexpr (is_decimal<T>)
        {
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); // restore
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound);
        }
        else
        {
            key += static_cast<T>(range_join_ctx.upper_bound); // restore
            key -= static_cast<T>(range_join_ctx.lower_bound);
        }

        /// >= key
        auto upper_iter = m->lower_bound(key);

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;
        if (is_left_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_left_strict)
            --upper_iter;

        assert(upper_iter->first <= key);
        assert(upper_iter->first >= lower_iter->first);

        if (!joined_rows
            || !joined_rows->contains(
                JoinTuple{src_block_id, upper_iter->second.block, static_cast<uint32_t>(row_num), upper_iter->second.row_num}))
            result = &upper_iter->second;
    };

    callWithType(type, call);
    return result;
}
}
}
