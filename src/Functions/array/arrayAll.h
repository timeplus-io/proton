#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include "FunctionArrayMapped.h"

/// proton: starts.
#include <DataTypes/DataTypeFactory.h>
/// proton: ends.

namespace DB
{

/** arrayAll(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for all elements of the array.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayAllImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        /// proton: starts. return bool
        return DataTypeFactory::instance().get("bool");
        /// proton: ends.
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped);
};

struct NameArrayAll { static constexpr auto name = "array_all"; };
using FunctionArrayAll = FunctionArrayMapped<ArrayAllImpl, NameArrayAll>;

}
