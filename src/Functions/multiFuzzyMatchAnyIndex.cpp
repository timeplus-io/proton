#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multi_fuzzy_match_any_index";
};

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAnyIndex, /*ResultType*/ UInt64, MultiMatchTraits::Find::AnyIndex, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAnyIndex)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
}

}
