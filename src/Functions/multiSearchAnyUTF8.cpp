#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAnyUTF8
{
    static constexpr auto name = "multi_search_any_utf8";
};
using FunctionMultiSearchUTF8 = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyUTF8, PositionCaseSensitiveUTF8>>;

}

void registerFunctionMultiSearchAnyUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchUTF8>();
}

}
