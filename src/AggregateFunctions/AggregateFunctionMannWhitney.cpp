#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMannWhitney.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionMannWhitneyUTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::NOT_IMPLEMENTED);

    return std::make_shared<AggregateFunctionMannWhitney>(argument_types, parameters);
}

}


void registerAggregateFunctionMannWhitney(AggregateFunctionFactory & factory)
{
    factory.registerFunction("mann_whitney_utest", createAggregateFunctionMannWhitneyUTest);
}

}
