#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "FunctionsURL.h"
#include "path.h"
#include <base/find_symbols.h>

namespace DB
{

struct NamePathFull { static constexpr auto name = "path_full"; };
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPath<true>>, NamePathFull>;

REGISTER_FUNCTION(PathFull)
{
    factory.registerFunction<FunctionPathFull>();
}

}
