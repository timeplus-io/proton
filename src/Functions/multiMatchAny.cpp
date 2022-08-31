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

using FunctionMultiMatchAny = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<NameMultiMatchAny, Bool, true, false, false>,
    std::numeric_limits<UInt32>::max()>;

}

void registerFunctionMultiMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
