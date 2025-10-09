#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

struct NameModuloOrNull { static constexpr auto name = "modulo_or_null"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull, false>;

REGISTER_FUNCTION(ModuloOrNull)
{
    factory.registerFunction<FunctionModuloOrNull>();
    factory.registerAlias("mod_or_null", "modulo_or_null", FunctionFactory::CaseInsensitive);
}

struct NamePositiveModuloOrNull
{
    static constexpr auto name = "positive_modulo_or_null";
};
using FunctionPositiveModuloOrNll = BinaryArithmeticOverloadResolver<PositiveModuloOrNullImpl, NamePositiveModuloOrNull, false>;

REGISTER_FUNCTION(PositiveModuloOrNull)
{
    factory.registerFunction<FunctionPositiveModuloOrNll>(FunctionDocumentation
        {
            .description = R"(
Calculates the remainder when dividing `a` by `b`. Similar to function `positiveModulo` except that `positiveModuloOrNull` will return NULL
if the right argument is 0.
        )",
            .examples{{"positive_modulo_or_null", "SELECT positive_modulo_or_null(1, 0);", ""}},
            .category{"Arithmetic"}},
        FunctionFactory::CaseInsensitive);

    factory.registerAlias("pmod_or_null", "positive_modulo_or_null", FunctionFactory::CaseInsensitive);
}

}
