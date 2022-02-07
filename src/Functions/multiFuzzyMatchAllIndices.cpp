#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAllIndicesImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAllIndices
{
    static constexpr auto name = "multi_fuzzy_match_all_indices";
};

using FunctionMultiFuzzyMatchAllIndices = FunctionsMultiStringFuzzySearch<
    MultiMatchAllIndicesImpl<NameMultiFuzzyMatchAllIndices, UInt64, true>,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiFuzzyMatchAllIndices(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>();
}

}
