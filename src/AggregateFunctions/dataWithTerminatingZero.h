#pragma once

#include <Columns/ColumnString.h>
#include <base/StringRef.h>

namespace DB
{
struct Compatibility
{
    /// Old versions used to store terminating null-character in SingleValueDataString.
    /// Then -WithTerminatingZero methods were removed from IColumn interface,
    /// because these methods are quite dangerous and easy to misuse. It introduced incompatibility.
    /// See https://github.com/ClickHouse/ClickHouse/pull/41431 and https://github.com/ClickHouse/ClickHouse/issues/42916
    /// Here we keep these functions for compatibility.
    /// It's safe because there's no way unsanitized user input (without \0 at the end) can reach these functions.

    static std::string_view getDataAtWithTerminatingZero(const ColumnString & column, size_t n)
    {
        auto res = column.getDataAt(n);
        /// ColumnString always reserves extra byte for null-character after string.
        /// But getDataAt returns StringRef without the null-character. Let's add it.
        chassert(res.data[res.size] == '\0');
        ++res.size;
        return res.toView();
    }

    static void insertDataWithTerminatingZero(ColumnString & column, const char * pos, size_t length)
    {
        /// String already has terminating null-character.
        /// But insertData will add another one unconditionally. Trim existing null-character to avoid duplication.
        chassert(0 < length);
        chassert(pos[length - 1] == '\0');
        column.insertData(pos, length - 1);
    }
};

}
