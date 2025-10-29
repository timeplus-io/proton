#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionThirdMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;

void registerAggregateFunctionsStatisticsThirdMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("skew_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skew_samp>);
    factory.registerFunction("skew_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skew_pop>);
}

}
