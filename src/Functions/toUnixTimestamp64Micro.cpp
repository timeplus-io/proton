#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Micro)
{
    factory.registerFunction("to_unix_timestamp64_micro",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(6, "to_unix_timestamp64_micro")); });
}

}
