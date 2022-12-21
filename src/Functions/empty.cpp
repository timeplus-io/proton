#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/EmptyImpl.h>


namespace DB
{
namespace
{

struct NameEmpty
{
    static constexpr auto name = "empty";
};

/// proton: starts. return bool
using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, /*result*/bool, false>;
/// proton: ends.

}

REGISTER_FUNCTION(Empty)
{
    factory.registerFunction<FunctionEmpty>();
}

}

