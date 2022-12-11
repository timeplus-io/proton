#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfMonthImpl>;

void registerFunctionToStartOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMonth>();
}

}


