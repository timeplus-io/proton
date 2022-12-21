#include <Functions/FunctionFactory.h>
#include "ExtractFirstSignificantSubdomain.h"
#include "FirstSignificantSubdomainCustomImpl.h"


namespace DB
{

struct NameFirstSignificantSubdomainCustom { static constexpr auto name = "first_significant_subdomain_custom"; };
using FunctionFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, false>, NameFirstSignificantSubdomainCustom>;

struct NameFirstSignificantSubdomainCustomRFC { static constexpr auto name = "first_significant_subdomain_custom_rfc"; };
using FunctionFirstSignificantSubdomainCustomRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, true>, NameFirstSignificantSubdomainCustomRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomainCustom)
{
    factory.registerFunction<FunctionFirstSignificantSubdomainCustom>();
    factory.registerFunction<FunctionFirstSignificantSubdomainCustomRFC>();
}

}
