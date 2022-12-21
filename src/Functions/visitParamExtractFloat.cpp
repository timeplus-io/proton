#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractFloat { static constexpr auto name = "simple_json_extract_float"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractFloat, ExtractNumericType<Float64>>>;

REGISTER_FUNCTION(VisitParamExtractFloat)
{
    factory.registerFunction<FunctionSimpleJSONExtractFloat>();
    factory.registerAlias("visit_param_extract_float", "simple_json_extract_float");
}

}
