#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionFourthMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 4>>;

void registerAggregateFunctionsStatisticsFourthMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("kurt_samp", createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurt_samp>);
    factory.registerFunction("kurt_pop", createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurt_pop>);
}

}
