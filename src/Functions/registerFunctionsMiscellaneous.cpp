#include <config_core.h>

namespace DB
{

class FunctionFactory;

#if USE_MISC_FUNCS
void registerFunctionCurrentDatabase(FunctionFactory &);
void registerFunctionCurrentProfiles(FunctionFactory &);
void registerFunctionCurrentRoles(FunctionFactory &);
void registerFunctionHostName(FunctionFactory &);
void registerFunctionFQDN(FunctionFactory &);
void registerFunctionVisibleWidth(FunctionFactory &);
void registerFunctionGetSizeOfEnumType(FunctionFactory &);
void registerFunctionBlockSerializedSize(FunctionFactory &);
void registerFunctionToColumnTypeName(FunctionFactory &);
void registerFunctionDumpColumnStructure(FunctionFactory &);
void registerFunctionDefaultValueOfArgumentType(FunctionFactory &);
void registerFunctionBlockSize(FunctionFactory &);
void registerFunctionBlockNumber(FunctionFactory &);
void registerFunctionRowNumberInBlock(FunctionFactory &);
void registerFunctionRowNumberInAllBlocks(FunctionFactory &);
void registerFunctionIgnore(FunctionFactory &);
void registerFunctionIndexHint(FunctionFactory &);
void registerFunctionIdentity(FunctionFactory &);
void registerFunctionBar(FunctionFactory &);
void registerFunctionHasColumnInTable(FunctionFactory &);
void registerFunctionIsFinite(FunctionFactory &);
void registerFunctionIsInfinite(FunctionFactory &);
void registerFunctionIsNaN(FunctionFactory &);
void registerFunctionIfNotFinite(FunctionFactory &);
void registerFunctionThrowIf(FunctionFactory &);
void registerFunctionBuildId(FunctionFactory &);
void registerFunctionUptime(FunctionFactory &);
void registerFunctionTimezoneOf(FunctionFactory &);
void registerFunctionRunningAccumulate(FunctionFactory &);
void registerFunctionRunningDifference(FunctionFactory &);
void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory &);
void registerFunctionRunningConcurrency(FunctionFactory &);
void registerFunctionToLowCardinality(FunctionFactory &);
void registerFunctionLowCardinalityIndices(FunctionFactory &);
void registerFunctionLowCardinalityKeys(FunctionFactory &);
void registerFunctionFilesystem(FunctionFactory &);
void registerFunctionEvalMLMethod(FunctionFactory &);
void registerFunctionBasename(FunctionFactory &);
void registerFunctionTransform(FunctionFactory &);
void registerFunctionGetMacro(FunctionFactory &);
void registerFunctionGetScalar(FunctionFactory &);
void registerFunctionGetSetting(FunctionFactory &);
void registerFunctionIsConstant(FunctionFactory &);
void registerFunctionIsDecimalOverflow(FunctionFactory &);
void registerFunctionCountDigits(FunctionFactory &);
void registerFunctionGlobalVariable(FunctionFactory &);
void registerFunctionHasThreadFuzzer(FunctionFactory &);
void registerFunctionErrorCodeToName(FunctionFactory &);
void registerFunctionTcpPort(FunctionFactory &);
void registerFunctionGetServerPort(FunctionFactory &);
void registerFunctionByteSize(FunctionFactory &);
void registerFunctionConnectionId(FunctionFactory & factory);
void registerFunctionPartitionId(FunctionFactory & factory);
void registerFunctionIsIPAddressContainedIn(FunctionFactory &);
void registerFunctionQueryID(FunctionFactory & factory);
void registerFunctionInitialQueryID(FunctionFactory & factory);
void registerFunctionServerUUID(FunctionFactory &);
void registerFunctionGetOSKernelVersion(FunctionFactory &);

#if USE_ICU
void registerFunctionConvertCharset(FunctionFactory &);
#endif

#endif

void registerFunctionCurrentUser(FunctionFactory &);
void registerFunctionTimezone(FunctionFactory &);
void registerFunctionVersion(FunctionFactory &);

void registerFunctionToTypeName(FunctionFactory &);
void registerFunctionMaterialize(FunctionFactory &);
void registerFunctionDefaultValueOfTypeName(FunctionFactory &);
void registerFunctionNeighbor(FunctionFactory &);
void registerFunctionArrayJoin(FunctionFactory &);
void registerFunctionReplicate(FunctionFactory &);
void registerFunctionFinalizeAggregation(FunctionFactory &);
void registerFunctionsIn(FunctionFactory &);
void registerFunctionJoinGet(FunctionFactory &);
void registerFunctionSleep(FunctionFactory &);
void registerFunctionSleepEachRow(FunctionFactory &);
void registerFunctionInitializeAggregation(FunctionFactory &);
void registerFunctionFile(FunctionFactory & factory);

/// proton: starts.
void registerFunctionStreamingNeighbor(FunctionFactory &);
/// proton: ends.

#ifdef FUZZING_MODE
void registerFunctionGetFuzzerData(FunctionFactory & factory);
#endif

void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
#if USE_MISC_FUNCS
    registerFunctionCurrentDatabase(factory);
    registerFunctionCurrentProfiles(factory);
    registerFunctionCurrentRoles(factory);
    registerFunctionHostName(factory);
    registerFunctionFQDN(factory);
    registerFunctionVisibleWidth(factory);
    registerFunctionGetSizeOfEnumType(factory);
    registerFunctionBlockSerializedSize(factory);
    registerFunctionToColumnTypeName(factory);
    registerFunctionDumpColumnStructure(factory);
    registerFunctionDefaultValueOfArgumentType(factory);
    registerFunctionBlockSize(factory);
    registerFunctionBlockNumber(factory);
    registerFunctionRowNumberInBlock(factory);
    registerFunctionRowNumberInAllBlocks(factory);
    registerFunctionIgnore(factory);
    registerFunctionIndexHint(factory);
    registerFunctionIdentity(factory);
    registerFunctionBar(factory);
    registerFunctionHasColumnInTable(factory);
    registerFunctionIsFinite(factory);
    registerFunctionIsInfinite(factory);
    registerFunctionIsNaN(factory);
    registerFunctionIfNotFinite(factory);
    registerFunctionThrowIf(factory);
    registerFunctionBuildId(factory);
    registerFunctionUptime(factory);
    registerFunctionTimezoneOf(factory);
    registerFunctionRunningAccumulate(factory);
    registerFunctionRunningDifference(factory);
    registerFunctionRunningDifferenceStartingWithFirstValue(factory);
    registerFunctionRunningConcurrency(factory);
    registerFunctionToLowCardinality(factory);
    registerFunctionLowCardinalityIndices(factory);
    registerFunctionLowCardinalityKeys(factory);
    registerFunctionFilesystem(factory);
    registerFunctionEvalMLMethod(factory);
    registerFunctionBasename(factory);
    registerFunctionTransform(factory);
    registerFunctionGetMacro(factory);
    registerFunctionGetScalar(factory);
    registerFunctionGetSetting(factory);
    registerFunctionIsConstant(factory);
    registerFunctionIsDecimalOverflow(factory);
    registerFunctionCountDigits(factory);
    registerFunctionGlobalVariable(factory);
    registerFunctionHasThreadFuzzer(factory);
    registerFunctionErrorCodeToName(factory);
    registerFunctionTcpPort(factory);
    registerFunctionGetServerPort(factory);
    registerFunctionByteSize(factory);
    registerFunctionFile(factory);
    registerFunctionConnectionId(factory);
    registerFunctionPartitionId(factory);
    registerFunctionIsIPAddressContainedIn(factory);
    registerFunctionQueryID(factory);
    registerFunctionInitialQueryID(factory);
    registerFunctionServerUUID(factory);
    registerFunctionGetOSKernelVersion(factory);

#if USE_ICU
    registerFunctionConvertCharset(factory);
#endif
#endif

    registerFunctionTimezone(factory);
    registerFunctionVersion(factory);

#ifdef FUZZING_MODE
    registerFunctionGetFuzzerData(factory);
#endif

    registerFunctionCurrentUser(factory);
    registerFunctionToTypeName(factory);
    registerFunctionMaterialize(factory);
    registerFunctionDefaultValueOfTypeName(factory);
    registerFunctionNeighbor(factory);
    registerFunctionSleep(factory);
    registerFunctionSleepEachRow(factory);
    registerFunctionArrayJoin(factory);
    registerFunctionReplicate(factory);
    registerFunctionFinalizeAggregation(factory);
    registerFunctionsIn(factory);
    registerFunctionJoinGet(factory);
    registerFunctionInitializeAggregation(factory);
    registerFunctionFile(factory);

    /// proton: starts.
    registerFunctionStreamingNeighbor(factory);
    /// proton: ends.
}

}
