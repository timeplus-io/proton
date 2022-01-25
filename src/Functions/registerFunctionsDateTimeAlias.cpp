#include <Functions/FunctionFactory.h>

namespace DB
{
void registerFunctionsDateTimeAlias(FunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'parseDateTimeBestEffortXXXX' functions
    factory.registerAlias("toTime", "parseDateTimeBestEffortUS");
}
}
