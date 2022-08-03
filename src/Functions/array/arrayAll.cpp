#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

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
        return std::make_shared<DataTypeBool>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnBool * column_filter = typeid_cast<const ColumnBool *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnBool>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
                return DataTypeBool().createColumnConst(array.size(), 1u);
            else
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnBool::create(offsets.size());
                ColumnBool::Container & out_all = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_all[i] = offsets[i] == pos;
                    pos = offsets[i];
                }

                return out_column;
            }
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnBool::create(offsets.size());
        ColumnBool::Container & out_all = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt8 all = 1;
            for (; pos < offsets[i]; ++pos)
            {
                if (!filter[pos])
                {
                    all = 0;
                    pos = offsets[i];
                    break;
                }
            }
            out_all[i] = all;
        }

        return out_column;
    }
};

struct NameArrayAll { static constexpr auto name = "array_all"; };
using FunctionArrayAll = FunctionArrayMapped<ArrayAllImpl, NameArrayAll>;

void registerFunctionArrayAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayAll>();
}

}


