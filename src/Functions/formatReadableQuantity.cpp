#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "format_readable_quantity";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableQuantity(value, out);
        }
    };
}

REGISTER_FUNCTION(FormatReadableQuantity)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>();
}

}
