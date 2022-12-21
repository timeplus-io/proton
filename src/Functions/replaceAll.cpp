#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{
namespace
{

struct NameReplaceAll
{
    static constexpr auto name = "replace_all";
};

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<ReplaceStringTraits::Replace::All>, NameReplaceAll>;

}

REGISTER_FUNCTION(ReplaceAll)
{
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
}

}
