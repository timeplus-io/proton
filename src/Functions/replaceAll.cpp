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

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;

}

void registerFunctionReplaceAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceAll>();
}

}
