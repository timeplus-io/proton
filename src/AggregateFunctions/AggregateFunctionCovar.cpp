#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T1, typename T2> using AggregateFunctionCovar = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CovarMoments>>;

void registerAggregateFunctionsStatisticsCovar(AggregateFunctionFactory & factory)
{
    factory.registerFunction("covar_samp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covar_samp>);
    factory.registerFunction("covar_pop", createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covar_pop>);
}

}
