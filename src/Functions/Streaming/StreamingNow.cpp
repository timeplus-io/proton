#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>

#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <ctime>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionNow : public IExecutableFunction
{
public:
    String getName() const override { return "now"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// NOTE: The `FilterTransform` will try optimizing filter ConstColumn to always_false or always_true,
        /// for exmaple: `now() < '2020-1-1 00:00:01'`, it will be optimized always_true or always_false.
        /// So we can not create a constant column, since the column data isn't constants value in fact.
        return result_type->createColumnConst(input_rows_count, static_cast<UInt64>(time(nullptr)))->convertToFullColumnIfConst();
    }
};

class FunctionBaseNow : public IFunctionBase
{
public:
    explicit FunctionBaseNow(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    String getName() const override { return "now"; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionNow>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};

class NowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "__streaming_now";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<NowOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0 or 1", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(
                "Arguments of function " + getName() + " should be string or fixed_string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (arguments.size() == 1)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0));
        }
        return std::make_shared<DataTypeDateTime>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0 or 1", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(
                "Arguments of function " + getName() + " should be string or fixed_string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (arguments.size() == 1)
            return std::make_unique<FunctionBaseNow>(
                DataTypes{arguments.front().type},
                std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0)));

        return std::make_unique<FunctionBaseNow>(DataTypes(), std::make_shared<DataTypeDateTime>());
    }
};

}

REGISTER_FUNCTION(StreamingNow)
{
    factory.registerFunction<NowOverloadResolver>();
}

}
