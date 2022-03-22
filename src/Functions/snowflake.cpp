#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerDateTimeToSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("datetime_to_snowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTimeToSnowflake>("datetime_to_snowflake")); });
}

void registerDateTime64ToSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("datetime64_to_snowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("datetime64_to_snowflake")); });
}

void registerSnowflakeToDateTime(FunctionFactory & factory)
{
    factory.registerFunction("snowflake_to_datetime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflake_to_datetime")); });
}
void registerSnowflakeToDateTime64(FunctionFactory & factory)
{
    factory.registerFunction("snowflake_to_datetime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflake_to_datetime64")); });
}

}
