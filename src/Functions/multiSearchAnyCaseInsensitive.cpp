#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitive
{
    static constexpr auto name = "multi_search_any_case_insensitive";
};
using FunctionMultiSearchCaseInsensitive = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitive)
{
    factory.registerFunction<FunctionMultiSearchCaseInsensitive>();
}

}
