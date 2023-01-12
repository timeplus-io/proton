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
