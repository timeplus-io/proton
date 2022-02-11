#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFromUnixTimestamp64Micro(FunctionFactory & factory)
{
    factory.registerFunction("from_unix_timestamp64_micro",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(6, "from_unix_timestamp64_micro")); });
}

}
