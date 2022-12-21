#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct HasParam
{
    /// proton: starts. return bool
    using ResultType = bool;
    /// proton: ends.

    static UInt8 extract(const UInt8 *, const UInt8 *)
    {
        return true;
    }
};

struct NameSimpleJSONHas { static constexpr auto name = "simple_json_has"; };
using FunctionSimpleJSONHas = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONHas, HasParam>>;

REGISTER_FUNCTION(VisitParamHas)
{
    factory.registerFunction<FunctionSimpleJSONHas>();
    factory.registerAlias("visit_param_has", "simple_json_has");
}

}
