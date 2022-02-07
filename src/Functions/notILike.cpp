#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameNotILike
{
    static constexpr auto name = "not_ilike";
};

using NotILikeImpl = MatchImpl<NameNotILike, true, true, /*case-insensitive*/true>;
using FunctionNotILike = FunctionsStringSearch<NotILikeImpl>;

}

void registerFunctionNotILike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotILike>();
}
}
