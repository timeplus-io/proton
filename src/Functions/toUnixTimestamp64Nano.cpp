#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Nano)
{
    factory.registerFunction("to_unix_timestamp64_nano",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(9, "to_unix_timestamp64_nano")); });
}

}
