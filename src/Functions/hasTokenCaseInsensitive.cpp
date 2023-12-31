#include "FunctionsStringSearch.h"
#include <Functions/FunctionFactory.h>
#include "HasTokenImpl.h"
#include <Common/Volnitsky.h>


namespace DB
{
namespace
{

struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "has_token_case_insensitive";
};

using FunctionHasTokenCaseInsensitive
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitive, VolnitskyCaseInsensitiveToken, false>>;

}

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>();
}

}
