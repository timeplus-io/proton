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

/** arrayExists(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for at least one array element.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayExistsImpl
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
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnBool::create(offsets.size());
                ColumnBool::Container & out_exists = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_exists[i] = offsets[i] - pos > 0;
                    pos = offsets[i];
                }

                return out_column;
            }
            else
                return DataTypeBool().createColumnConst(array.size(), 0u);
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnBool::create(offsets.size());
        ColumnBool::Container & out_exists = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt8 exists = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                {
                    exists = 1;
                    pos = offsets[i];
                    break;
                }
            }
            out_exists[i] = exists;
        }

        return out_column;
    }
};

struct NameArrayExists { static constexpr auto name = "array_exists"; };
using FunctionArrayExists = FunctionArrayMapped<ArrayExistsImpl, NameArrayExists>;

void registerFunctionArrayExists(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayExists>();
}

}


