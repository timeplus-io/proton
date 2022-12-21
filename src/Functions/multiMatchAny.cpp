#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAny
{
    static constexpr auto name = "multi_match_any";
};

/// proton: starts. return bool
using FunctionMultiMatchAny = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAny, /*ResultType*/ bool, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;
/// proton: ends.

}

REGISTER_FUNCTION(MultiMatchAny)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
