#include "AggregateFunctionCount.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace Streaming
{

namespace
{

AggregateFunctionPtr
createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    /// the last position reserved for `delta` col, and one col for the data input.
    if (name.ends_with("retract"))
        assertArityAtMost<2>(name, argument_types);
    else
        assertArityAtMost<1>(name, argument_types);

    return std::make_shared<AggregateFunctionCount>(argument_types);
}

}

AggregateFunctionPtr AggregateFunctionCount::getOwnNullAdapter(
    const AggregateFunctionPtr &, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const
{
    return std::make_shared<AggregateFunctionCountNotNullUnary>(types[0], params);
}

void registerAggregateFunctionCountRetract(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true, .is_order_dependent = false};
    factory.registerFunction("__count_retract", {createAggregateFunctionCount, properties}, AggregateFunctionFactory::CaseSensitive);
}

}
}
