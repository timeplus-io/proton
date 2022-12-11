#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinutesImpl>;

void registerFunctionToStartOfFiveMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFiveMinutes>();
}

}


