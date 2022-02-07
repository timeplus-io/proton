#include <Functions/FunctionFactory.h>
#include "ExtractFirstSignificantSubdomain.h"
#include "FirstSignificantSubdomainCustomImpl.h"


namespace DB
{

struct NameFirstSignificantSubdomainCustom { static constexpr auto name = "first_significant_subdomain_custom"; };

using FunctionFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true>, NameFirstSignificantSubdomainCustom>;

void registerFunctionFirstSignificantSubdomainCustom(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFirstSignificantSubdomainCustom>();
}

}
