#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractUInt   { static constexpr auto name = "simple_json_extract_uint"; };
using FunctionSimpleJSONExtractUInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractUInt, ExtractNumericType<UInt64>>>;

void registerFunctionVisitParamExtractUInt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSimpleJSONExtractUInt>();
}

}
