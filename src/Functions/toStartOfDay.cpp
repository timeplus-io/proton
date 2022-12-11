#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayImpl>;

void registerFunctionToStartOfDay(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfDay>();
}

}


