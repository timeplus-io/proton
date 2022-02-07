#include <Functions/FunctionFactory.h>
#include "arrayEnumerateRanked.h"


namespace DB
{

class FunctionArrayEnumerateDenseRanked : public FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>
{
    using Base = FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>;

public:
    static constexpr auto name = "array_enumerate_dense_ranked";
    using Base::create;
};

void registerFunctionArrayEnumerateDenseRanked(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateDenseRanked>();
}

}
