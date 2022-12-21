#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToLastDayOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToLastDayOfMonthImpl>;

REGISTER_FUNCTION(ToLastDayOfMonth)
{
    factory.registerFunction<FunctionToLastDayOfMonth>();

    /// MySQL compatibility alias.
    factory.registerAlias("LAST_DAY", "to_last_day_of_month", FunctionFactory::CaseInsensitive);
}

}
