#include "HelpersMinMaxAny.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{
namespace Streaming
{
namespace
{
AggregateFunctionPtr
createAggregateFunctionMax(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionCountedValue<AggregateFunctionsCountedValue, AggregateFunctionMaxData, true>(
        name, argument_types, parameters, settings));
}
}

void registerAggregateFunctionsMaxRetract(AggregateFunctionFactory & factory)
{
    factory.registerFunction("__max_retract", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);
}
}
}
