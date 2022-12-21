#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractUInt   { static constexpr auto name = "simple_json_extract_uint"; };
using FunctionSimpleJSONExtractUInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractUInt, ExtractNumericType<UInt64>>>;


REGISTER_FUNCTION(VisitParamExtractUInt)
{
    factory.registerFunction<FunctionSimpleJSONExtractUInt>();
    factory.registerAlias("visit_param_extract_uint", "simple_json_extract_uint");
}

}
