#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Milli)
{
    factory.registerFunction("from_unix_timestamp64_milli",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(3, "from_unix_timestamp64_milli")); });
}

}
