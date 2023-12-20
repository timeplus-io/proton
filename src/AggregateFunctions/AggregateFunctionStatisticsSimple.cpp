#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


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

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<FunctionTemplate>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<FunctionTemplate>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory & factory)
{
    factory.registerFunction("var_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionVarSampSimple>);
    factory.registerFunction("var_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionVarPopSimple>);
    factory.registerFunction("stddev_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevSampSimple>);
    factory.registerFunction("stddev_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionStddevPopSimple>);
    factory.registerFunction("skew_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionSkewSampSimple>);
    factory.registerFunction("skew_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionSkewPopSimple>);
    factory.registerFunction("kurt_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionKurtSampSimple>);
    factory.registerFunction("kurt_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionKurtPopSimple>);

    factory.registerFunction("covar_samp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarSampSimple>);
    factory.registerFunction("covar_pop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovarPopSimple>);
    factory.registerFunction("corr", createAggregateFunctionStatisticsBinary<AggregateFunctionCorrSimple>, AggregateFunctionFactory::CaseInsensitive);
}

}
