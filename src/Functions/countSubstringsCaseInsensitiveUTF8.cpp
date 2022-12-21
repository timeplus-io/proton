#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitiveUTF8
{
    static constexpr auto name = "count_substrings_case_insensitive_utf8";
};

using FunctionCountSubstringsCaseInsensitiveUTF8 = FunctionsStringSearch<
        CountSubstringsImpl<NameCountSubstringsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitiveUTF8>();
}
}
