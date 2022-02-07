#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

struct NameFirstSignificantSubdomain { static constexpr auto name = "first_significant_subdomain"; };

using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true>>, NameFirstSignificantSubdomain>;

void registerFunctionFirstSignificantSubdomain(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFirstSignificantSubdomain>();
}

}
