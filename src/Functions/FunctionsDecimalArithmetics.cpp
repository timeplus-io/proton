#include <Functions/FunctionsDecimalArithmetics.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(DivideDecimals)
{
    factory.registerFunction<FunctionsDecimalArithmetics<DivideDecimalsImpl>>({
        .description = "Decimal division with given precision. Slower than simple `divide`, but has controlled precision and no sound overflows"
    });
}

REGISTER_FUNCTION(MultiplyDecimals)
{
    factory.registerFunction<FunctionsDecimalArithmetics<MultiplyDecimalsImpl>>({
        .description = "Decimal multiplication with given precision. Slower than simple `divide`, but has controlled precision and no sound overflows"
    });
}
}
