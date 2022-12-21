#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(DateTimeToSnowflake)
{
    factory.registerFunction("datetime_to_snowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTimeToSnowflake>("datetime_to_snowflake")); });
}

REGISTER_FUNCTION(DateTime64ToSnowflake)
{
    factory.registerFunction("datetime64_to_snowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("datetime64_to_snowflake")); });
}

REGISTER_FUNCTION(SnowflakeToDateTime)
{
    factory.registerFunction("snowflake_to_datetime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflake_to_datetime")); });
}
REGISTER_FUNCTION(SnowflakeToDateTime64)
{
    factory.registerFunction("snowflake_to_datetime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflake_to_datetime64")); });
}

}
