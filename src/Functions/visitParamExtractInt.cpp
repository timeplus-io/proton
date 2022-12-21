#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractInt { static constexpr auto name = "simple_json_extract_int"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractInt, ExtractNumericType<Int64>>>;

REGISTER_FUNCTION(VisitParamExtractInt)
{
    factory.registerFunction<FunctionSimpleJSONExtractInt>();
    factory.registerAlias("visit_param_extract_int", "simple_json_extract_int");
}

}
