#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multi_search_all_positions_case_insensitive_utf8";
};

using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>();
}

}
