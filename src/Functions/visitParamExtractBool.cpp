#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct ExtractBool
{
    /// proton: starts. return bool
    using ResultType = bool;
    /// proton: ends.

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return begin + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(begin), "true", 4);
    }
};

struct NameSimpleJSONExtractBool   { static constexpr auto name = "simple_json_extract_bool"; };
using FunctionSimpleJSONExtractBool = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractBool, ExtractBool>>;

REGISTER_FUNCTION(VisitParamExtractBool)
{
    factory.registerFunction<FunctionSimpleJSONExtractBool>();
    factory.registerAlias("visit_param_extract_bool", "simple_json_extract_bool");
}

}
