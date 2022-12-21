#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/EmptyImpl.h>


namespace DB
{
namespace
{

struct NameNotEmpty
{
    static constexpr auto name = "not_empty";
};

/// proton: starts. return bool
using FunctionNotEmpty = FunctionStringOrArrayToT<EmptyImpl<true>, NameNotEmpty, /*result*/bool, false>;
/// proton: ends.

}

REGISTER_FUNCTION(NotEmpty)
{
    factory.registerFunction<FunctionNotEmpty>();
}

}
