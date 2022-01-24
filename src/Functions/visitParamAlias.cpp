#include <Functions/FunctionFactory.h>

namespace DB
{

void registerJSONFunctionAlias(FunctionFactory & factory)
{
    /// rename JSON processing related functions
    /// 'visitParamXXXX' functions
    factory.registerAlias("jsonExtractBool", "visitParamExtractBool");
    factory.registerAlias("jsonExtractFloat", "visitParamExtractFloat");
    factory.registerAlias("jsonExtractInt", "visitParamExtractInt");
    factory.registerAlias("jsonExtractString", "visitParamExtractString");

    /// 'JSONExtractXXXX' functions
    factory.registerAlias("jsonExtractArray", "JSONExtractArrayRaw");
    factory.registerAlias("jsonPath", "JSON_VALUE");
}

}
