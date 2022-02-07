#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitive
{
    static constexpr auto name = "count_substrings_case_insensitive";
};

using FunctionCountSubstringsCaseInsensitive = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

void registerFunctionCountSubstringsCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitive>();
}
}
