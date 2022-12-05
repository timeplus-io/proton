#include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
#add_headers_and_sources(clickhouse_aggregate_functions .)

#list(REMOVE_ITEM clickhouse_aggregate_functions_sources
#    IAggregateFunction.cpp
#    AggregateFunctionFactory.cpp
#    AggregateFunctionCombinatorFactory.cpp
#    AggregateFunctionCount.cpp
#    AggregateFunctionState.cpp
#    parseAggregateFunctionParameters.cpp
#    FactoryHelpers.cpp
#)
#
#list(REMOVE_ITEM clickhouse_aggregate_functions_headers
#    IAggregateFunction.h
#    IAggregateFunctionCombinator.h
#    AggregateFunctionFactory.h
#    AggregateFunctionCombinatorFactory.h
#    AggregateFunctionCount.h
#    AggregateFunctionState.h
#    parseAggregateFunctionParameters.h
#    FactoryHelpers.h
#)

#AggregateFunctionAggThrow.cpp
#AggregateFunctionAny.cpp
#AggregateFunctionArgMinMax.h
#AggregateFunctionArray.cpp
#AggregateFunctionArray.h
#AggregateFunctionAvg.cpp
#AggregateFunctionAvg.h
#AggregateFunctionAvgWeighted.cpp
#AggregateFunctionAvgWeighted.h
#AggregateFunctionBitwise.cpp
#AggregateFunctionBitwise.h
#AggregateFunctionBoundingRatio.cpp
#AggregateFunctionBoundingRatio.h
#AggregateFunctionCategoricalInformationValue.cpp
#AggregateFunctionCategoricalInformationValue.h
#AggregateFunctionCombinatorFactory.cpp
#AggregateFunctionCombinatorFactory.h
#AggregateFunctionContingencyCoefficient.cpp
#AggregateFunctionCount.cpp
#AggregateFunctionCount.h
#AggregateFunctionCramersVBiasCorrected.cpp
#AggregateFunctionCramersV.cpp
#AggregateFunctionDeltaSum.cpp
#AggregateFunctionDeltaSum.h
#AggregateFunctionDeltaSumTimestamp.cpp
#AggregateFunctionDeltaSumTimestamp.h
#AggregateFunctionDistinct.cpp
#AggregateFunctionDistinct.h
#AggregateFunctionDistinctStreaming.cpp
#AggregateFunctionDistinctStreaming.h
#AggregateFunctionEntropy.cpp
#AggregateFunctionEntropy.h
#AggregateFunctionExponentialMovingAverage.cpp
#AggregateFunctionFactory.cpp
#AggregateFunctionFactory.h
#AggregateFunctionForEach.cpp
#AggregateFunctionForEach.h
#AggregateFunctionGroupArray.cpp
#AggregateFunctionGroupArray.h
#AggregateFunctionGroupArrayInsertAt.cpp
#AggregateFunctionGroupArrayInsertAt.h
#AggregateFunctionGroupArrayMoving.cpp
#AggregateFunctionGroupArrayMoving.h
#AggregateFunctionGroupBitmap.cpp
#AggregateFunctionGroupBitmapData.h
#AggregateFunctionGroupBitmap.h
#AggregateFunctionGroupUniqArray.cpp
#AggregateFunctionGroupUniqArray.h
#AggregateFunctionHistogram.cpp
#AggregateFunctionHistogram.h
#AggregateFunctionIf.cpp
#AggregateFunctionIf.h
#AggregateFunctionIntervalLengthSum.cpp
#AggregateFunctionIntervalLengthSum.h
#AggregateFunctionMannWhitney.cpp
#AggregateFunctionMannWhitney.h
#AggregateFunctionMap.cpp
#AggregateFunctionMap.h
#AggregateFunctionMax.cpp
#AggregateFunctionMaxIntersections.cpp
#AggregateFunctionMaxIntersections.h
#AggregateFunctionMeanZTest.cpp
#AggregateFunctionMeanZTest.h
#AggregateFunctionMerge.cpp
#AggregateFunctionMerge.h
#AggregateFunctionMin.cpp
#AggregateFunctionMinMaxAny.h
#AggregateFunctionMinMaxK.cpp
#AggregateFunctionMinMaxK.h
#AggregateFunctionMLMethod.cpp
#AggregateFunctionMLMethod.h
#AggregateFunctionNothing.cpp
#AggregateFunctionNothing.h
#AggregateFunctionNull.cpp
#AggregateFunctionNull.h
#AggregateFunctionOrFill.cpp
#AggregateFunctionOrFill.h
#AggregateFunctionQuantile.cpp
#AggregateFunctionQuantile.h
#AggregateFunctionRankCorrelation.cpp
#AggregateFunctionRankCorrelation.h
#AggregateFunctionResample.cpp
#AggregateFunctionResample.h
#AggregateFunctionRetention.cpp
#AggregateFunctionRetention.h
#AggregateFunctionSequenceMatch.cpp
#AggregateFunctionSequenceMatch.h
#AggregateFunctionSequenceNextNode.cpp
#AggregateFunctionSequenceNextNode.h
#AggregateFunctionSimpleLinearRegression.cpp
#AggregateFunctionSimpleLinearRegression.h
#AggregateFunctionSimpleState.cpp
#AggregateFunctionSimpleState.h
#AggregateFunctionSingleValueOrNull.cpp
#AggregateFunctionSparkbar.cpp
#AggregateFunctionSparkbar.h
#AggregateFunctionState.cpp
#AggregateFunctionState.h
#AggregateFunctionStatistics.cpp
#AggregateFunctionStatistics.h
#AggregateFunctionStatisticsSimple.cpp
#AggregateFunctionStatisticsSimple.h
#AggregateFunctionStudentTTest.cpp
#AggregateFunctionSumCount.cpp
#AggregateFunctionSumCount.h
#AggregateFunctionSum.cpp
#AggregateFunctionSum.h
#AggregateFunctionSumMap.cpp
#AggregateFunctionSumMap.h
#AggregateFunctionTheilsU.cpp
#AggregateFunctionTopK.cpp
#AggregateFunctionTopK.h
#AggregateFunctionTTest.h
#AggregateFunctionUniqCombined.cpp
#AggregateFunctionUniqCombined.h
#AggregateFunctionUniq.cpp
#AggregateFunctionUniq.h
#AggregateFunctionUniqUpTo.cpp
#AggregateFunctionUniqUpTo.h
#AggregateFunctionWelchTTest.cpp
#AggregateFunctionWindowFunnel.cpp
#AggregateFunctionWindowFunnel.h
#CrossTab.h
#FactoryHelpers.h
#Helpers.h
#HelpersMinMaxAny.h
#IAggregateFunctionCombinator.h
#IAggregateFunction.cpp
#IAggregateFunction.h
#KeyHolderHelpers.h
#Moments.h
#parseAggregateFunctionParameters.cpp
#parseAggregateFunctionParameters.h
#QuantileBFloat16Histogram.h
#QuantileExact.h
#QuantileExactWeighted.h
#QuantileReservoirSamplerDeterministic.h
#QuantileReservoirSampler.h
#QuantilesCommon.h
#QuantileTDigest.h
#QuantileTiming.h
#registerAggregateFunctionsAlias.cpp
#registerAggregateFunctions.cpp
#registerAggregateFunctions.h
#ReservoirSamplerDeterministic.h
#ReservoirSampler.h
#StatCommon.h
#ThetaSketchData.h
#UniqCombinedBiasData.cpp
#UniqCombinedBiasData.h
#UniquesHashSet.h
#UniqVariadicHash.cpp
#UniqVariadicHash.h


set (clickhouse_aggregate_functions_sources
        AggregateFunctionAny.cpp
        AggregateFunctionAvg.cpp
        AggregateFunctionAvgWeighted.cpp
        AggregateFunctionArray.cpp
        AggregateFunctionCount.cpp
        AggregateFunctionDistinct.cpp
        AggregateFunctionDistinctStreaming.cpp
        AggregateFunctionGroupArray.cpp
        AggregateFunctionGroupArrayMoving.cpp
        AggregateFunctionHistogram.cpp
        AggregateFunctionIf.cpp
        AggregateFunctionMin.cpp
        AggregateFunctionMax.cpp
        AggregateFunctionMinMaxK.cpp
        AggregateFunctionQuantile.cpp
        AggregateFunctionSimpleState.cpp
        AggregateFunctionState.cpp
        AggregateFunctionMerge.cpp
        AggregateFunctionSum.cpp
        AggregateFunctionSumCount.cpp
        AggregateFunctionTopK.cpp
        AggregateFunctionUniq.cpp
        AggregateFunctionXirr.cpp
        registerAggregateFunctionsAlias.cpp
        registerAggregateFunctions.cpp
        UniqVariadicHash.cpp
)


add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
