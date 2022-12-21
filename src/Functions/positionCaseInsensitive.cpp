#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionCaseInsensitive
{
    static constexpr auto name = "position_case_insensitive";
};

using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(PositionCaseInsensitive)
{
    factory.registerFunction<FunctionPositionCaseInsensitive>();
}
}
