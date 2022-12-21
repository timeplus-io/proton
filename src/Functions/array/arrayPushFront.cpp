#include "arrayPush.h"
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayPushFront : public FunctionArrayPush
{
public:
    static constexpr auto name = "array_push_front";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPushFront>(); }
    FunctionArrayPushFront() : FunctionArrayPush(true, name) {}
};


REGISTER_FUNCTION(ArrayPushFront)
{
    factory.registerFunction<FunctionArrayPushFront>();
}

}
