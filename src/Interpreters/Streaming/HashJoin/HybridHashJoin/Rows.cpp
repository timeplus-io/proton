#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/Rows.h>
#include <Interpreters/Streaming/HashJoin/RowUtil.h>
#include <Interpreters/Streaming/OneRow.h>
#include <Common/typeid_cast.h>

namespace DB::Streaming
{

void MutableRows::insert(Block & block, size_t row)
{
    rows.emplace_back();
    rows.back().resize(block.columns());

    OneRow::update(block, row, rows.back());
}

void MutableRows::serialize(WriteBuffer & wb) const
{
    writeVarUInt(rows.size(), wb);

    if (!rows.empty())
    {
        auto row_size = rows.back().size();
        writeVarUInt(row_size, wb);

        for (const auto & row : rows)
        {
            chassert(row.size() == row_size);
            for (const auto & field : row)
                writeFieldBinary(field, wb);
        }
    }
}

void MutableRows::deserialize(ReadBuffer & rb)
{
    uint64_t number_of_rows = 0;
    readVarUInt(number_of_rows, rb);

    if (number_of_rows)
    {
        uint64_t row_size = 0;
        readVarUInt(row_size, rb);

        for (size_t i = 0; i < number_of_rows; ++i)
        {
            rows.emplace_back();
            auto & current_row = rows.back();
            current_row.reserve(row_size);
            for (size_t j = 0; j < row_size; ++j)
                current_row.emplace_back(readFieldBinary(rb));
        }
    }
}

void RowList::insert(Block & block, size_t row)
{
    rows.emplace_back();
    rows.back().resize(block.columns());
    OneRow::update(block, row, rows.back());
}

void RowList::serialize(WriteBuffer & wb) const
{
    writeVarUInt(rows.size(), wb);

    if (!rows.empty())
    {
        writeVarUInt(rows.front().size(), wb);
        for (const auto & row : rows)
            for (const auto & col : row)
                writeFieldBinary(col, wb);
    }
}

void RowList::deserialize(ReadBuffer & rb)
{
    uint64_t num_rows = 0;
    readVarUInt(num_rows, rb);

    if (num_rows != 0)
    {
        size_t row_size = 0;
        readVarUInt(row_size, rb);

        for (size_t i = 0; i < num_rows; ++i)
        {
            rows.emplace_back();
            auto & row = rows.back();
            row.reserve(row_size);

            for (size_t j = 0; j < row_size; ++j)
                row.emplace_back(readFieldBinary(rb));
        }
    }
}

AsofRows::AsofRows(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;
        lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

void AsofRows::insert(
    TypeIndex type, const IColumn & asof_column, Block & block, size_t row_num, ASOFJoinInequality inequality, size_t keep_versions)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupPtr = typename Entry<T>::LookupPtr;

        auto & container = std::get<LookupPtr>(lookups);

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        /// First insert to row list
        rows.insert(block, row_num);

        T key = column.getElement(row_num);
        bool ascending = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::LessOrEquals);
        auto removed_entry = container->insert(Entry<T>{key, RowListRef{.iter = rows.lastRowIter()}}, ascending, keep_versions);
        if (removed_entry)
        {
            rows.remove(removed_entry->row_ref);
            chassert(rows.rows.size() <= keep_versions);
        }
    };

    callWithType(type, call);
}

const RowListRef * AsofRows::findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const
{
    const RowListRef * out = nullptr;

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

void AsofRows::serialize(WriteBuffer & wb, TypeIndex type) const
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        const auto & sorted_lookup_vec = *std::get<typename Entry<T>::LookupPtr>(lookups);
        writeVarUInt(sorted_lookup_vec.size(), wb);
        if (sorted_lookup_vec.size() == 0)
            return;

        auto row_size = sorted_lookup_vec.begin()->row_ref.iter.value()->size();
        writeVarUInt(row_size, wb);
        for (const auto & [asof_value, row_list_ref] : sorted_lookup_vec)
        {
            /// Key
            writeBinary(asof_value, wb);

            /// Mapped: RowListRef, write real row data
            chassert(row_list_ref.iter);
            const auto & row = **row_list_ref.iter;
            for (const auto & field : row)
                writeFieldBinary(field, wb);
        }
    };
    callWithType(type, call);
}

void AsofRows::deserialize(ReadBuffer & rb, TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        auto & sorted_lookup_vec = *std::get<typename Entry<T>::LookupPtr>(lookups);

        size_t vec_size;
        readVarUInt(vec_size, rb);
        if (vec_size == 0)
            return;

        size_t row_size;
        readVarUInt(row_size, rb);

        sorted_lookup_vec.resize(vec_size);
        for (auto & [asof_value, row_list_ref] : sorted_lookup_vec)
        {
            /// Key
            readBinary(asof_value, rb);

            /// Mapped: RowListRef, read row data and insert to row list
            rows.rows.emplace_back();
            auto & row = rows.rows.back();
            row.reserve(row_size);

            for (size_t j = 0; j < row_size; ++j)
                row.emplace_back(readFieldBinary(rb));

            row_list_ref.iter = rows.lastRowIter();
        }
    };
    callWithType(type, call);
}

RangeAsofRows::RangeAsofRows(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        lookups = std::make_unique<LookupType<T>>();
    };

    callWithType(type, call);
}

void RangeAsofRows::insert(TypeIndex type, const IColumn & asof_column, Block & block, size_t row_num)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        /// First save row in row list
        rows.insert(block, row_num);

        T key = column.getElement(row_num);
        std::get<LookupPtr<T>>(lookups)->emplace(key, RowListRef{.iter = rows.lastRowIter()});
    };

    callWithType(type, call);
}

std::vector<RowListRef> RangeAsofRows::findRange(
    TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num, bool is_left_block) const
{
    std::vector<RowListRef> results;

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

        chassert(upper_iter->first >= lower_iter->first);

        do
        {
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

        chassert(upper_iter->first >= lower_iter->first);

        do
        {
            results.push_back(lower_iter->second);
        } while (lower_iter++ != upper_iter); /// We need include value at upper_iter, so postfix lower_iter++
    };

    if (is_left_block)
        callWithType(type, call_for_left_block);
    else
        callWithType(type, call_for_right_block);
    return results;
}

const RowListRef *
RangeAsofRows::findAsof(TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num) const
{
    RowListRef * result = nullptr;

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

        chassert(lower_iter->first >= key);

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

        chassert(upper_iter->first <= key);
        chassert(upper_iter->first >= lower_iter->first);

        result = &upper_iter->second;
    };

    callWithType(type, call);
    return result;
}

void RangeAsofRows::serialize(WriteBuffer & wb, TypeIndex type) const
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        const auto & map = *std::get<LookupPtr<T>>(lookups);

        writeVarUInt(map.size(), wb);
        if (map.empty())
            return;

        auto row_size = map.begin()->second.iter.value()->size();
        writeVarUInt(row_size, wb);

        for (const auto & [key, row_list_ref] : map)
        {
            /// Key
            writeBinary(key, wb);

            /// Mapped: RowListRef, write real row data
            chassert(row_list_ref.iter);
            const auto & row = **row_list_ref.iter;
            for (const auto & field : row)
                writeFieldBinary(field, wb);
        }
    };
    callWithType(type, call);
}

void RangeAsofRows::deserialize(ReadBuffer & rb, TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        static_assert(!std::is_same_v<T, StringRef>);
        auto & map = *std::get<LookupPtr<T>>(lookups);

        T key;
        size_t map_size;
        readVarUInt(map_size, rb);
        if (map_size == 0)
            return;

        size_t row_size;
        readVarUInt(row_size, rb);

        for (size_t i = 0; i < map_size; ++i)
        {
            /// Key
            DB::readBinary(key, rb);

            /// Mapped: RowListRef, read row data and insert to row list
            rows.rows.emplace_back();
            auto & row = rows.rows.back();
            row.reserve(row_size);

            for (size_t j = 0; j < row_size; ++j)
                row.emplace_back(readFieldBinary(rb));

            // chassert(!map.contains(key)); // multimap allows multiple same keys
            map.emplace(key, rows.lastRowIter());
        }
    };
    callWithType(type, call);
}

}
