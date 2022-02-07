#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB
{
namespace
{

struct NameCountSubstrings
{
    static constexpr auto name = "count_substrings";
};

using FunctionCountSubstrings = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstrings, PositionCaseSensitiveASCII>>;

}

void registerFunctionCountSubstrings(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountSubstrings>(FunctionFactory::CaseInsensitive);
}
}
