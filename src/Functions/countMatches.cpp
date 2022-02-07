#include "FunctionFactory.h"
#include "countMatches.h"

namespace
{

struct FunctionCountMatchesCaseSensitive
{
    static constexpr auto name = "count_matches";
    static constexpr bool case_insensitive = false;
};
struct FunctionCountMatchesCaseInsensitive
{
    static constexpr auto name = "count_matches_case_insensitive";
    static constexpr bool case_insensitive = true;
};

}

namespace DB
{

void registerFunctionCountMatches(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseSensitive>>(FunctionFactory::CaseSensitive);
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseInsensitive>>(FunctionFactory::CaseSensitive);
}

}
