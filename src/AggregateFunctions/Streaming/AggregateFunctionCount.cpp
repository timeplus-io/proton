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
    /// Reserves the last position for the 'delta' column and one column for data input.
    /// A replacement has been implemented: `count_retract()` to `count_retract(_tp_delta)`.
    /// But, it's also necessary to consider: `count_retract(data)` to `count_retract(data, _tp_delta)`.
    assertArityAtMost<2>(name, argument_types);

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
    factory.registerFunction("__count_retract", {createAggregateFunctionCount, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
}
