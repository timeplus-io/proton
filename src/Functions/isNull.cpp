#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{
namespace
{

/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull : public IFunction
{
public:
    static constexpr auto name = "is_null";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeBool>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*elem.column))
        {
            auto res_column = ColumnBool::create(input_rows_count, 1u);
            assert_cast<ColumnBool &>(*res_column).applyZeroMap(nullable->getNullMapData(), true);
            return res_column;
        }
        else
        {
            /// Since no element is nullable, return a zero-constant column representing
            /// a zero-filled null map.
            return DataTypeBool().createColumnConst(elem.column->size(), 0u);
        }
    }
};

}

void registerFunctionIsNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNull>(FunctionFactory::CaseInsensitive);
}

}
