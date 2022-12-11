#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFifteenMinutesImpl>;

void registerFunctionToStartOfFifteenMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
}

}


