#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPosition
{
    static constexpr auto name = "multi_search_first_position";
};

using FunctionMultiSearchFirstPosition
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<NameMultiSearchFirstPosition, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstPosition)
{
    factory.registerFunction<FunctionMultiSearchFirstPosition>();
}

}
