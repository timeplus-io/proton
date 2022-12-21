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

using FunctionMultiFuzzyMatchAllIndices = FunctionsMultiStringFuzzySearch<MultiMatchAllIndicesImpl<NameMultiFuzzyMatchAllIndices, /*ResultType*/ UInt64, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAllIndices)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>();
}

}
