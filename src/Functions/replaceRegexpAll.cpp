#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceRegexpImpl.h"


namespace DB
{
namespace
{

struct NameReplaceRegexpAll
{
    static constexpr auto name = "replace_regexp_all";
};

using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<ReplaceRegexpTraits::Replace::All>, NameReplaceRegexpAll>;

}

REGISTER_FUNCTION(ReplaceRegexpAll)
{
    factory.registerFunction<FunctionReplaceRegexpAll>();
    factory.registerAlias("replace_regexp", NameReplaceRegexpAll::name);
    factory.registerAlias("replace_regex", NameReplaceRegexpAll::name);
}

}
