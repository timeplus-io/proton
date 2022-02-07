#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToWeekImpl>;
using FunctionToYearWeek = FunctionCustomWeekToSomething<DataTypeUInt32, ToYearWeekImpl>;
using FunctionToStartOfWeek = FunctionCustomWeekToSomething<DataTypeDate, ToStartOfWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToWeek>();
    factory.registerFunction<FunctionToYearWeek>();
    factory.registerFunction<FunctionToStartOfWeek>();
}

}
