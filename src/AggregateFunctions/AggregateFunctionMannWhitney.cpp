#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMannWhitney.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Settings.h>

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
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    assertBinary(name, argument_types);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);


    bool use_arena = settings ? settings->default_hash_table != HashTableType::Hybrid : true;
    if (use_arena)
        return std::make_shared<AggregateFunctionMannWhitney<true>>(argument_types, parameters);
    else
        return std::make_shared<AggregateFunctionMannWhitney<false>>(argument_types, parameters);
}

}


void registerAggregateFunctionMannWhitney(AggregateFunctionFactory & factory)
{
    factory.registerFunction("mann_whitney_utest", createAggregateFunctionMannWhitneyUTest);
}

}
