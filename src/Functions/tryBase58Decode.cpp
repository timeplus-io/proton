#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct NameTryBase58Decode
{
    static constexpr auto name = "try_base58_decode";
};

using TryBase58DecodeImpl = Base58Decode<NameTryBase58Decode, Base58DecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase58Decode = FunctionBase58Conversion<TryBase58DecodeImpl>;

}

REGISTER_FUNCTION(TryBase58Decode)
{
    factory.registerFunction<FunctionTryBase58Decode>();
}

}
