#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceRegexpImpl.h"


namespace DB
{
namespace
{

struct NameReplaceRegexpAll
{
    static constexpr auto name = "replace_regex";
};

using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;

}

void registerFunctionReplaceRegexpAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceRegexpAll>();
}

}
