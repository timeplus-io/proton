#include "FunctionsMultiStringFuzzySearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multi_fuzzy_match_any";
};

/// proton: starts. return bool
using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAny, /*ResultType*/ bool, MultiMatchTraits::Find::Any, /*WithEditDistance*/ true>>;
/// proton: ends.

}

REGISTER_FUNCTION(MultiFuzzyMatchAny)
{
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
}

}
