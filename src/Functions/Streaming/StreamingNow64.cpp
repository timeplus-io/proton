#include <DataTypes/DataTypeDateTime64.h>

#include <Core/DecimalFunctions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeNullable.h>

#include <Common/assert_cast.h>
#include <Common/timeScale.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_CLOCK_GETTIME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionNow64 : public IExecutableFunction
{
public:
    explicit ExecutableFunctionNow64(Int32 scale_) : scale(scale_) {}

    String getName() const override { return "now64"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (unlikely(input_rows_count == 0))
            return result_type->createColumn();

        return result_type->createColumnConst(input_rows_count, nowSubsecond(scale));
    }

private:
    UInt32 scale;
};

class FunctionBaseNow64 : public IFunctionBase
{
public:
    explicit FunctionBaseNow64(UInt32 scale_, DataTypes argument_types_, DataTypePtr return_type_)
        : scale(scale_), argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    String getName() const override { return "now64"; }

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
        return std::make_unique<ExecutableFunctionNow64>(scale);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }

private:
    UInt32 scale;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class Now64OverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto * name = "__streaming_now64";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<Now64OverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        UInt32 scale = DataTypeDateTime64::default_scale;
        String timezone_name;

        if (arguments.size() > 2)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0, or 1, or 2", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (!arguments.empty())
        {
            const auto & argument = arguments[0];
            if (!isInteger(argument.type) || !argument.column || !isColumnConst(*argument.column))
                throw Exception("Illegal type " + argument.type->getName() +
                                " of 0" +
                                " argument of function " + getName() +
                                ". Expected const integer.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            scale = static_cast<UInt32>(argument.column->get64(0));
        }
        if (arguments.size() == 2)
        {
            timezone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        }

        return std::make_shared<DataTypeDateTime64>(scale, timezone_name);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        UInt32 scale = DataTypeDateTime64::default_scale;
        auto res_type = removeNullable(result_type);
        if (const auto * type = typeid_cast<const DataTypeDateTime64 *>(res_type.get()))
            scale = type->getScale();

        DataTypes arg_types;
        arg_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            arg_types.push_back(arg.type);

        return std::make_unique<FunctionBaseNow64>(scale, std::move(arg_types), result_type);
    }
};

}

REGISTER_FUNCTION(StreamingNow64)
{
    factory.registerFunction<Now64OverloadResolver>();
}

}
