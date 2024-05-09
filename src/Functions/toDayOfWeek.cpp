#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>

#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfWeekImpl>;

REGISTER_FUNCTION(ToDayOfWeek)
{
    factory.registerFunction<FunctionToDayOfWeek>();

    /// MysQL compatibility alias.
    factory.registerAlias("day_of_week", "to_day_of_week", FunctionFactory::CaseInsensitive);
}

}
