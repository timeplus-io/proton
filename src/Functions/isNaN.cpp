#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct IsNaNImpl
{
    static constexpr auto name = "is_nan";
    template <typename T>
    static bool execute(const T t)
    {
        /// Suppression for PVS-Studio.
        return t != t;  //-V501
    }
};

using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;

}

REGISTER_FUNCTION(IsNaN)
{
    factory.registerFunction<FunctionIsNaN>();
}

}
