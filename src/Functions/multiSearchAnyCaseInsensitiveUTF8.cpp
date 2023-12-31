#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitiveUTF8
{
    static constexpr auto name = "multi_search_any_case_insensitive_utf8";
};

using FunctionMultiSearchCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitiveUTF8>();
}

}
