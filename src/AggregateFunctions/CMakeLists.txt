include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)

extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
    IAggregateFunction.cpp
    AggregateFunctionFactory.cpp
    AggregateFunctionJavaScriptAdapter.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionState.cpp
    AggregateFunctionCount.cpp
    parseAggregateFunctionParameters.cpp
)
extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
    IAggregateFunction.h
    AggregateFunctionJavaScriptAdapter.h
    IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    AggregateFunctionCombinatorFactory.h
    AggregateFunctionState.h
    AggregateFunctionCount.h
    FactoryHelpers.h
    parseAggregateFunctionParameters.h
)

if (ENABLE_PYTHON_UDF)
    extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
        AggregateFunctionPythonAdapter.cpp
    )
    extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
        AggregateFunctionPythonAdapter.h
    )
endif()

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_headers} ${clickhouse_aggregate_functions_sources})

if (TARGET ch_contrib::jemalloc)
    target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash ch_contrib::jemalloc)
else()
    target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)
endif()

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
