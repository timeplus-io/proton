include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)

list(REMOVE_ITEM clickhouse_aggregate_functions_sources
    IAggregateFunction.cpp
    AggregateFunctionFactory.cpp
    AggregateFunctionJavaScriptAdapter.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionCount.cpp
    AggregateFunctionState.cpp
    parseAggregateFunctionParameters.cpp
    FactoryHelpers.cpp
)

list(REMOVE_ITEM clickhouse_aggregate_functions_headers
    IAggregateFunction.h
    AggregateFunctionJavaScriptAdapter.cpp
    IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    AggregateFunctionCombinatorFactory.h
    AggregateFunctionCount.h
    AggregateFunctionState.h
    parseAggregateFunctionParameters.h
    FactoryHelpers.h
)

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
