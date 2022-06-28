#include "config_core.h"

#include <Functions/FunctionFactory.h>


namespace DB
{
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsArray(FunctionFactory &);
void registerFunctionsTuple(FunctionFactory &);
void registerFunctionsMap(FunctionFactory &);

#if USE_BITMAP_FUNCS
void registerFunctionsBitmap(FunctionFactory &);
#endif
#if USE_BINARY_REPR_FUNCS
void registerFunctionsBinaryRepr(FunctionFactory &);
#endif
#if USE_IP_CODING_FUNCS
void registerFunctionsCoding(FunctionFactory &);
#endif
#if USE_UUID_CODING_FUNCS
void registerFunctionsCodingUUID(FunctionFactory &);
#endif
void registerFunctionChar(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionCastOrDefault(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsEmbeddedDictionaries(FunctionFactory &);
#if USE_EXTERNAL_DICT_FUNCS
void registerFunctionsExternalDictionaries(FunctionFactory &);
#endif
#if USE_EXTERNAL_MODELS_FUNCS
void registerFunctionsExternalModels(FunctionFactory &);
#endif
#if USE_FORMATTING_FUNCS
void registerFunctionsFormatting(FunctionFactory &);
#endif
#if USE_HASH_FUNCS
void registerFunctionsHashing(FunctionFactory &);
#endif
#if USE_HIGH_ORDER_ARRAY_FUNCS
void registerFunctionsHigherOrder(FunctionFactory &);
#endif
void registerFunctionsLogical(FunctionFactory &);
void registerFunctionsMiscellaneous(FunctionFactory &);
void registerFunctionsRandom(FunctionFactory &);
void registerFunctionsReinterpret(FunctionFactory &);
void registerFunctionsRound(FunctionFactory &);
void registerFunctionsString(FunctionFactory &);
void registerFunctionsStringArray(FunctionFactory &);
void registerFunctionsStringSearch(FunctionFactory &);
void registerFunctionsStringRegexp(FunctionFactory &);
void registerFunctionsStringSimilarity(FunctionFactory &);
void registerFunctionsStringTokenExtractor(FunctionFactory &);
#if USE_URL_FUNCS
void registerFunctionsURL(FunctionFactory &);
#endif
void registerFunctionsVisitParam(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
#if USE_INTROSPECTION_FUNCS
void registerFunctionsIntrospection(FunctionFactory &);
#endif
void registerFunctionsNull(FunctionFactory &);
void registerFunctionsJSON(FunctionFactory &);
void registerFunctionsSQLJSON(FunctionFactory &);
void registerFunctionToJSONString(FunctionFactory &);
#if USE_CONSISTENT_HASH_FUNCS
void registerFunctionsConsistentHashing(FunctionFactory & factory);
#endif
void registerFunctionsUnixTimestamp64(FunctionFactory & factory);
#if USE_HAMMING_DISTANCE_FUNCS
void registerFunctionBitHammingDistance(FunctionFactory & factory);
void registerFunctionTupleHammingDistance(FunctionFactory & factory);
#endif
void registerFunctionsStringHash(FunctionFactory & factory);
void registerFunctionValidateNestedArraySizes(FunctionFactory & factory);
#if USE_SNOWFLAKE_FUNCS
void registerFunctionsSnowflake(FunctionFactory & factory);
#endif

/// proton: starts
void registerFunctionsStreamingWindow(FunctionFactory &);
void registerFunctionEmitVersion(FunctionFactory &);
void registerFunctionDedup(FunctionFactory &);
void registerFunctionGrok(FunctionFactory &);
/// proton: ends

#if !defined(ARCADIA_BUILD)
void registerFunctionBayesAB(FunctionFactory &);
#endif

void registerFunctionTid(FunctionFactory & factory);
void registerFunctionLogTrace(FunctionFactory & factory);
void registerFunctionToBool(FunctionFactory &);

#if USE_SSL
#if USE_ENCRYPT_DECRYPT_FUNCS
void registerFunctionEncrypt(FunctionFactory & factory);
void registerFunctionDecrypt(FunctionFactory & factory);
void registerFunctionAESEncryptMysql(FunctionFactory & factory);
void registerFunctionAESDecryptMysql(FunctionFactory & factory);
#endif
#endif

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    registerFunctionsArithmetic(factory);
    registerFunctionsArray(factory);
    registerFunctionsTuple(factory);
    registerFunctionsMap(factory);
#if USE_BITMAP_FUNCS
    registerFunctionsBitmap(factory);
#endif
#if USE_BINARY_REPR_FUNCS
    registerFunctionsBinaryRepr(factory);
#endif
#if USE_IP_CODING_FUNCS
    registerFunctionsCoding(factory);
#endif
#if USE_UUID_CODING_FUNCS
    registerFunctionsCodingUUID(factory);
#endif
    registerFunctionChar(factory);
    registerFunctionsComparison(factory);
    registerFunctionsConditional(factory);
    registerFunctionsConversion(factory);
    registerFunctionCastOrDefault(factory);
    registerFunctionsDateTime(factory);
    registerFunctionsEmbeddedDictionaries(factory);
#if USE_EXTERNAL_DICT_FUNCS
    registerFunctionsExternalDictionaries(factory);
#endif
#if USE_EXTERNAL_MODELS_FUNCS
    registerFunctionsExternalModels(factory);
#endif
#if USE_FORMATTING_FUNCS
    registerFunctionsFormatting(factory);
#endif
#if USE_HASH_FUNCS
    registerFunctionsHashing(factory);
#endif
#if USE_HIGH_ORDER_ARRAY_FUNCS
    registerFunctionsHigherOrder(factory);
#endif
    registerFunctionsLogical(factory);
    registerFunctionsMiscellaneous(factory);
    registerFunctionsRandom(factory);
    registerFunctionsReinterpret(factory);
    registerFunctionsRound(factory);
    registerFunctionsString(factory);
    registerFunctionsStringArray(factory);
    registerFunctionsStringSearch(factory);
    registerFunctionsStringRegexp(factory);
    registerFunctionsStringSimilarity(factory);
    registerFunctionsStringTokenExtractor(factory);
#if USE_URL_FUNCS
    registerFunctionsURL(factory);
#endif
    registerFunctionsVisitParam(factory);
    registerFunctionsMath(factory);
    registerFunctionsGeo(factory);
    registerFunctionsNull(factory);
    registerFunctionsJSON(factory);
    registerFunctionsSQLJSON(factory);
    registerFunctionToJSONString(factory);
#if USE_INTROSPECTION_FUNCS
    registerFunctionsIntrospection(factory);
#endif
#if USE_CONSISTENT_HASH_FUNCS
    registerFunctionsConsistentHashing(factory);
#endif
    registerFunctionsUnixTimestamp64(factory);
#if USE_HAMMING_DISTANCE_FUNCS
    registerFunctionBitHammingDistance(factory);
    registerFunctionTupleHammingDistance(factory);
#endif
    registerFunctionsStringHash(factory);
    registerFunctionValidateNestedArraySizes(factory);
#if USE_SNOWFLAKE_FUNCS
    registerFunctionsSnowflake(factory);
#endif
    registerFunctionToBool(factory);

    /// proton: starts
    registerFunctionsStreamingWindow(factory);
    registerFunctionEmitVersion(factory);
    registerFunctionDedup(factory);
    registerFunctionGrok(factory);
    /// proton: ends

#if USE_SSL
#if USE_ENCRYPT_DECRYPT_FUNCS
    registerFunctionEncrypt(factory);
    registerFunctionDecrypt(factory);
    registerFunctionAESEncryptMysql(factory);
    registerFunctionAESDecryptMysql(factory);
#endif
#endif

#if USE_DEBUG_FUNCS
    registerFunctionTid(factory);
    registerFunctionLogTrace(factory);
#endif
}

}
