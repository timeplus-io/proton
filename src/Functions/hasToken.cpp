#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasTokenImpl.h>

#include <Common/Volnitsky.h>

namespace DB
{

struct NameHasToken
{
    static constexpr auto name = "has_token";
};

using FunctionHasToken = DB::FunctionsStringSearch<DB::HasTokenImpl<NameHasToken, DB::VolnitskyCaseSensitiveToken, false>>;

REGISTER_FUNCTION(HasToken)
{
    factory.registerFunction<FunctionHasToken>(FunctionDocumentation
        {.description="Performs lookup of needle in haystack using tokenbf_v1 index."}, DB::FunctionFactory::CaseSensitive);
}

}
