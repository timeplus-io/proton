#include "AggregateFunctionFactory.h"

namespace DB
{
void registerAggregateFunctionsAlias(AggregateFunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'uniq_XXXX' functions
    factory.registerAlias("uniq", "unique");
    factory.registerAlias("uniq_exact", "unique_exact");
}
}
