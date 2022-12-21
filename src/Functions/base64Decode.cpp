#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(Base64Decode)
{
    tb64ini(0, 0);
    factory.registerFunction<FunctionBase64Conversion<Base64Decode>>();

    /// MysQL compatibility alias.
    factory.registerAlias("from_base64", "base64_decode", FunctionFactory::CaseSensitive);
}
}

#endif
