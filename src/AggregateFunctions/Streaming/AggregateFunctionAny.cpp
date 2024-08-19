#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Streaming/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/Streaming/HelpersMinMaxAny.h>

#include <string>

namespace DB
{
namespace Streaming
{
AggregateFunctionPtr createAggregateFunctionAnyLast(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(
        name, argument_types, parameters, settings));
}

AggregateFunctionPtr
createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(
        name, argument_types, parameters, settings));
}

void registerAggregateFunctionsAnyRetract(AggregateFunctionFactory & factory)
{
    /// FIXME:actually there is no retraction at all, we just ignore the data whose delta is -1
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("any_last_retract", {createAggregateFunctionAnyLast, properties});
    factory.registerFunction("any_retract", {createAggregateFunctionAny, properties});

    factory.registerFunction("first_value_retract", {createAggregateFunctionAny, properties}, AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("last_value_retract", {createAggregateFunctionAnyLast, properties}, AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("earliest_retract", {createAggregateFunctionAny, properties}, AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("latest_retract", {createAggregateFunctionAnyLast, properties}, AggregateFunctionFactory::CaseInsensitive);
}
}
}
