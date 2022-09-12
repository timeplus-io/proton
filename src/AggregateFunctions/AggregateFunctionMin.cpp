#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionArgMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsMin(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseSensitive);

    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("arg_min", { createAggregateFunctionArgMin, properties });
}

}
