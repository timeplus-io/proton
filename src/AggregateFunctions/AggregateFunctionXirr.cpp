#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionXirr.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr
createAggregateFunctionXirr(const String & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (arguments.size() != 2 && arguments.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function {}", name);

    DataTypePtr value_type = arguments[0];
    DataTypePtr date_type = arguments[1];

    if ((isInteger(value_type) || isFloat(value_type)) && isDateOrDate32(date_type))
        return AggregateFunctionPtr(
            createWithTwoNumericOrDateTypes<AggregationFunctionXirr>(*value_type, *date_type, arguments, params));
    else if (!(isInteger(value_type) || isFloat(value_type)))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type  {} of argument for aggregate function {}", arguments[0]->getName(), name);
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type  {} of argument for aggregate function {}", arguments[1]->getName(), name);
}
}

void registerAggregateFunctionXirr(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true, .is_order_dependent = true};

    factory.registerFunction("xirr", {createAggregateFunctionXirr, properties});
}

}
