#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>


namespace DB
{

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction<CastInternalOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseSensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    factory.registerFunction<CastOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseSensitive);
    factory.registerFunction<CastOverloadResolver<CastType::accurate>>();
    factory.registerFunction<CastOverloadResolver<CastType::accurateOrNull>>();
}

}
