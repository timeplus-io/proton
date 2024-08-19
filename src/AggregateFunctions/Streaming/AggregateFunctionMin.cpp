#include "HelpersMinMaxAny.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{
namespace Streaming
{
namespace
{
AggregateFunctionPtr
createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionCountedValue<AggregateFunctionsCountedValue, AggregateFunctionMinData, false>(
        name, argument_types, parameters, settings));
}
}

void registerAggregateFunctionsMinRetract(AggregateFunctionFactory & factory)
{
    factory.registerFunction("__min_retract", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);
}
}
}
