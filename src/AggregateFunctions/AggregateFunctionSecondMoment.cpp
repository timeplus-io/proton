#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionSecondMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;

void registerAggregateFunctionsStatisticsSecondMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("var_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::var_samp>);
    factory.registerFunction("var_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::var_pop>);
    factory.registerFunction("stddev_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddev_samp>);
    factory.registerFunction("stddev_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddev_pop>);
}

}