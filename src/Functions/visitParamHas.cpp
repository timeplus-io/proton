#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct HasParam
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 *, const UInt8 *)
    {
        return true;
    }
};

struct NameSimpleJSONHas           { static constexpr auto name = "simple_json_has"; };
using FunctionSimpleJSONHas = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONHas, HasParam>>;

void registerFunctionVisitParamHas(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSimpleJSONHas>();
}

}
