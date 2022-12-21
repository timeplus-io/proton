#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multi_match_any_index";
};

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAnyIndex, /*ResultType*/ UInt64, MultiMatchTraits::Find::AnyIndex, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAnyIndex)
{
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
}

}
