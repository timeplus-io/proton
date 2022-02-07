#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameCountEqual { static constexpr auto name = "count_equal"; };

using FunctionCountEqual = FunctionArrayIndex<CountEqualAction, NameCountEqual>;

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}
}
