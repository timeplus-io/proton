#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfWeekImpl>;

void registerFunctionToDayOfWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToDayOfWeek>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayOfWeek>("DAYOFWEEK", FunctionFactory::CaseInsensitive);

    /// proton: starts.
    factory.registerFunction<FunctionToDayOfWeek>("weekday", FunctionFactory::CaseSensitive);
    /// proton: ends.
}

}


