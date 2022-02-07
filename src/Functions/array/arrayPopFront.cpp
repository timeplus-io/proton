#include "arrayPop.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopFront : public FunctionArrayPop
{
public:
    static constexpr auto name = "array_pop_front";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPopFront>(); }
    FunctionArrayPopFront() : FunctionArrayPop(true, name) {}
};

void registerFunctionArrayPopFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

}
