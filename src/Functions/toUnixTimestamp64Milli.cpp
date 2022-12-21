#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Milli)
{
    factory.registerFunction("to_unix_timestamp64_milli",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(3, "to_unix_timestamp64_milli")); });
}

}
