#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFromUnixTimestamp64Nano(FunctionFactory & factory)
{
    factory.registerFunction("from_unix_timestamp64_nano",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(9, "from_unix_timestamp64_nano")); });
}

}
