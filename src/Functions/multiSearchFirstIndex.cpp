#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndex
{
    static constexpr auto name = "multi_search_first_index";
};

using FunctionMultiSearchFirstIndex = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndex, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndex)
{
    factory.registerFunction<FunctionMultiSearchFirstIndex>();
}

}
