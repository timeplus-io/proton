#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourImpl>;

void registerFunctionToStartOfHour(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfHour>();
}

}


