#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatistics.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename, typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoBasicNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types));
    if (!res)
        throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
            + " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory & factory)
{
    factory.registerFunction("var_samp_stable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampStable>);
    factory.registerFunction("var_pop_stable", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopStable>);
    factory.registerFunction("stddev_samp_stable", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampStable>);
    factory.registerFunction("stddev_pop_stable", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopStable>);
    factory.registerFunction("covar_samp_stable", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampStable>);
    factory.registerFunction("covar_pop_stable", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopStable>);
    factory.registerFunction("corr_stable", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrStable>);
}

}
