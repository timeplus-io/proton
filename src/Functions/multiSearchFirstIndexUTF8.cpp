#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multi_search_first_index_utf8";
};

using FunctionMultiSearchFirstIndexUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexUTF8)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>();
}

}
