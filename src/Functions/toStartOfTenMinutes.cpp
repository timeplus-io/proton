#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfTenMinutesImpl>;

void registerFunctionToStartOfTenMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfTenMinutes>();
}

}


