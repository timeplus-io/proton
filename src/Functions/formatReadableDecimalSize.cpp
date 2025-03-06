#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "format_readable_decimal_size";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableSizeWithDecimalSuffix(value, out);
        }
    };
}

REGISTER_FUNCTION(FormatReadableDecimalSize)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>(
    FunctionDocumentation{
        .description=R"(
Accepts the size (number of bytes). Returns a rounded size with a suffix (KB, MB, etc.) as a string.
)",
        .examples{
            {"formatReadableDecimalSize", "SELECT format_readable_decimal_size(1000)", ""}},
        .categories{"OtherFunctions"}
    },
    FunctionFactory::CaseSensitive);
}

}
