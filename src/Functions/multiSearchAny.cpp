#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAny
{
    static constexpr auto name = "multi_search_any";
};

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAny, PositionCaseSensitiveASCII>>;

}

void registerFunctionMultiSearchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearch>();
}

}
