#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "domain.h"

namespace DB
{

struct NameDomainWithoutWWW { static constexpr auto name = "domain_without_www"; };
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true>>, NameDomainWithoutWWW>;


void registerFunctionDomainWithoutWWW(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDomainWithoutWWW>();
}

}
