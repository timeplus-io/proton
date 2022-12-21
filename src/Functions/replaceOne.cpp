#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{
namespace
{

struct NameReplaceOne
{
    static constexpr auto name = "replace_one";
};

using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<ReplaceStringTraits::Replace::First>, NameReplaceOne>;

}

REGISTER_FUNCTION(ReplaceOne)
{
    factory.registerFunction<FunctionReplaceOne>();
}

}
