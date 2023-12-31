#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/map.h>

#include <numeric>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/**
 * Function monotonic() returns a monotonically increasing sequence of numbers.
 * Now the argument range is min(Int64) ~ max(Int64).
 * So it supports passing uint8, uint16, uint32, int8, int16, int32, int64 as argument.
*/
class FuntionMonotonic : public IFunction
{
public:
    explicit FuntionMonotonic(Int64 start_num_) : start_num(start_num_) { }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isStateful() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto column = ColumnInt64::create(input_rows_count);
        std::iota(column->getData().begin(), column->getData().end(), start_num);
        start_num += input_rows_count;
        return column;
    }

    void serialize(WriteBuffer & wb) const override { writeIntBinary<Int64>(start_num, wb); }

    void deserialize(ReadBuffer & rb) const override { readIntBinary<Int64>(start_num, rb); }

    String getName() const override { return "monotonic"; }

private:
    mutable Int64 start_num;
};

class MonotonicOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "monotonic";

    String getName() const override { return name; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<MonotonicOverloadResolver>(); }

    size_t getNumberOfArguments() const override { return 1; }

    bool isStateful() const override { return true; }

    bool isVariadic() const override { return false; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " requires 1 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isInteger(arguments[0].type))
            throw Exception(
                fmt::format("Function {} requires integer argumengt but given {}", getName(), arguments[0].type->getFamilyName()),
                ErrorCodes::BAD_ARGUMENTS);

        return std::make_unique<DataTypeInt64>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!isColumnConst(*arguments[0].column))
            throw Exception("Function " + getName() + " requires constant argument", ErrorCodes::BAD_ARGUMENTS);

        auto start_num = assert_cast<const ColumnConst &>(*arguments[0].column).getInt(0);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            std::make_unique<FuntionMonotonic>(start_num),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            return_type);
    }
};


REGISTER_FUNCTION(Monotonic)
{
    factory.registerFunction<MonotonicOverloadResolver>({}, FunctionFactory::CaseInsensitive);
}
}