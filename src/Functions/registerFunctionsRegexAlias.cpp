#include <Functions/FunctionFactory.h>

namespace DB
{
void registerFunctionsRegexAlias(FunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'parseDateTimeBestEffortXXXX' functions
    factory.registerAlias("replaceRegex", "replaceRegexpAll");
}
}
