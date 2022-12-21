#include "arrayEnumerateExtended.h"
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayEnumerateDense : public FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>
{
    using Base = FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>;
public:
    static constexpr auto name = "array_enumerate_dense";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateDense)
{
    factory.registerFunction<FunctionArrayEnumerateDense>();
}

}
