#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionUTF8
{
    static constexpr auto name = "position_utf8";
};

using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<NamePositionUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(PositionUTF8)
{
    factory.registerFunction<FunctionPositionUTF8>();
}

}
