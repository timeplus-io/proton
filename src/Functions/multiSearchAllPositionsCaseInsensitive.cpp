#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitive
{
    static constexpr auto name = "multi_search_all_positions_case_insensitive";
};

using FunctionMultiSearchAllPositionsCaseInsensitive
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitive)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitive>();
}

}
