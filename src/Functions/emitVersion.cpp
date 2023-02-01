#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FUNCTION_IS_SPECIAL;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// emit_version() - a special function which cannot executed directly
///             is used only to get the result type of the corresponding expression
class FunctionEmitVersion : public IFunction
{
public:
    static constexpr auto name = "emit_version";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionEmitVersion>(); }

    /// Get the function name.
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    /** It could return many different values for single argument. */
    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be array.", getName());

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        throw Exception(ErrorCodes::FUNCTION_IS_SPECIAL, "Function {} must not be executed directly.", getName());
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override { return false; }
};

REGISTER_FUNCTION(EmitVersion)
{
    factory.registerFunction<FunctionEmitVersion>();
}

}
