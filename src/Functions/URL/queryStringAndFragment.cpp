#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "queryStringAndFragment.h"

namespace DB
{

struct NameQueryStringAndFragment { static constexpr auto name = "query_string_and_fragment"; };
using FunctionQueryStringAndFragment = FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true>>, NameQueryStringAndFragment>;

void registerFunctionQueryStringAndFragment(FunctionFactory & factory)
{
    factory.registerFunction<FunctionQueryStringAndFragment>();
}

}
