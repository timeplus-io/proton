#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToDateOrDate32<ToStartOfQuarterImpl>;

void registerFunctionToStartOfQuarter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfQuarter>();
}

}


