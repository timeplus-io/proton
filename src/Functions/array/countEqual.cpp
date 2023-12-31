#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameCountEqual { static constexpr auto name = "count_equal"; };

using FunctionCountEqual = FunctionArrayIndex<CountEqualAction, NameCountEqual>;

REGISTER_FUNCTION(CountEqual)
{
    factory.registerFunction<FunctionCountEqual>();
}
}
