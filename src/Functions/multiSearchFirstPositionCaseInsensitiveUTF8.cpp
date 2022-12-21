#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionCaseInsensitiveUTF8
{
    static constexpr auto name = "multi_search_first_position_case_insensitive_utf8";
};

using FunctionMultiSearchFirstPositionCaseInsensitiveUTF8 = FunctionsMultiStringSearch<
    MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstPositionCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitiveUTF8>();
}

}
