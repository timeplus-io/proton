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

void registerFunctionArrayEnumerateDense(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateDense>();
}

}
