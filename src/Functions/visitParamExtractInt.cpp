#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{
struct NameSimpleJSONExtractInt    { static constexpr auto name = "simple_json_extract_int"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractInt, ExtractNumericType<Int64>>>;

void registerFunctionVisitParamExtractInt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSimpleJSONExtractInt>();
}

}
