#include "AggregateFunctionFactory.h"

namespace DB
{
void registerAggregateFunctionsAlias(AggregateFunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'uniqXXXX' functions
    factory.registerAlias("uniq", "unique");
    factory.registerAlias("uniqExact", "uniqueExact");
}
}
