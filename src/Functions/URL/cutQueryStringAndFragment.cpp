#include <Functions/FunctionFactory.h>
#include "queryStringAndFragment.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryStringAndFragment { static constexpr auto name = "cut_query_string_and_fragment"; };
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false>>, NameCutQueryStringAndFragment>;

REGISTER_FUNCTION(CutQueryStringAndFragment)
{
    factory.registerFunction<FunctionCutQueryStringAndFragment>();
}

}
