#include "AggregateFunctionFactory.h"

namespace DB
{
void registerAggregateFunctionsAlias(AggregateFunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'uniqXXXX' functions
    factory.registerAlias("unique", "uniq");
    factory.registerAlias("uniqueExact", "uniqExact");
}
}
