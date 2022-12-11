#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfYearImpl>;

void registerFunctionToStartOfYear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfYear>();
}

}


