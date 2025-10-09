#pragma once

#include <Common/HashTable/HashTableKeyHolder.h>
#include <Columns/IColumn.h>

/// proton: starts.
#include <IO/WriteBufferFromString.h>
/// proton: ends.

namespace DB
{
struct Settings;

template <bool is_plain_column = false>
static ALWAYS_INLINE auto getKeyHolder(const IColumn & column, size_t row_num, Arena & arena)
{
    if constexpr (is_plain_column)
    {
        return ArenaKeyHolder{column.getDataAt(row_num), arena};
    }
    else
    {
        const char * begin = nullptr;
        StringRef serialized = column.serializeValueIntoArena(row_num, arena, begin);
        assert(serialized.data != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

/// proton: starts.
template <bool is_plain_column = false>
static ALWAYS_INLINE auto getKeyHolder(const IColumn & column, size_t row_num)
{
    if constexpr (is_plain_column)
    {
        return column.getDataAt(row_num);
    }
    else
    {
        WriteBufferFromOwnString buf;
        column.serializeValueIntoBuffer(row_num, buf);
        return buf.str();
    }
}
/// proton: ends.

template <bool is_plain_column>
static void deserializeAndInsert(StringRef str, IColumn & data_to)
{
    if constexpr (is_plain_column)
        data_to.insertData(str.data, str.size);
    else
        data_to.deserializeAndInsertFromArena(str.data);
}

}
