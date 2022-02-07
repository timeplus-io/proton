#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsUTF8
{
    static constexpr auto name = "multi_search_all_positions_utf8";
};

using FunctionMultiSearchAllPositionsUTF8
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseSensitiveUTF8>, NameMultiSearchAllPositionsUTF8>;

}

void registerFunctionMultiSearchAllPositionsUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsUTF8>();
}

}
