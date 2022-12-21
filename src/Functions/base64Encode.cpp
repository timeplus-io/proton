#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBase64Conversion.h>

#include "config.h"

#if USE_BASE64
#    include <DataTypes/DataTypeString.h>

namespace DB
{
void registerFunctionBase64Encode(FunctionFactory & factory)
{
    tb64ini(0, 0);
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>("to_base64", FunctionFactory::CaseSensitive);
}
}
#endif
