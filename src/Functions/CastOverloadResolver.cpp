#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>


namespace DB
{

void registerCastOverloadResolvers(FunctionFactory & factory)
{
    factory.registerFunction<CastInternalOverloadResolver<CastType::nonAccurate>>(FunctionFactory::CaseSensitive);
    factory.registerFunction<CastInternalOverloadResolver<CastType::accurate>>();
    factory.registerFunction<CastInternalOverloadResolver<CastType::accurateOrNull>>();

    factory.registerFunction<CastOverloadResolver<CastType::nonAccurate>>(FunctionFactory::CaseSensitive);
    factory.registerFunction<CastOverloadResolver<CastType::accurate>>();
    factory.registerFunction<CastOverloadResolver<CastType::accurateOrNull>>();
}

}
