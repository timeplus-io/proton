#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBoundingRatio.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionRate(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    if (argument_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionBoundingRatio>(argument_types);
}

}

void registerAggregateFunctionRate(AggregateFunctionFactory & factory)
{
    factory.registerFunction("bounding_ratio", createAggregateFunctionRate);
}

}
