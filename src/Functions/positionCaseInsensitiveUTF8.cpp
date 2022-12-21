#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionCaseInsensitiveUTF8
{
    static constexpr auto name = "position_case_insensitive_utf8";
};

using FunctionPositionCaseInsensitiveUTF8
    = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(PositionCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>();
}

}
