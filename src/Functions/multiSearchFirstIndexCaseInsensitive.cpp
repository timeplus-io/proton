#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitive
{
    static constexpr auto name = "multi_search_first_index_case_insensitive";
};

using FunctionMultiSearchFirstIndexCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

void registerFunctionMultiSearchFirstIndexCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitive>();
}

}
