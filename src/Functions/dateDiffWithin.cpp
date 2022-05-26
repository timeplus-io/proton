#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    class FunctionDateDiffWithin : public IFunction
    {
    public:
        static constexpr auto name = "date_diff_within";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateDiffWithin>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
        {
            return std::make_shared<DataTypeUInt64>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
        {
            if (input_rows_count != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} shall not be invoked directly", getName());

            return nullptr;
        }
    };
}

void registerFunctionDateDiffWithin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDateDiffWithin>(FunctionFactory::CaseSensitive);
}

}
