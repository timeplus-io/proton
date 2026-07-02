include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)

if (NOT ENABLE_AGGREGATES_EXTENDED)
    # add_headers_and_sources globs with the pattern `./*.cpp` so entries look
    # like `<dir>/./<file>.cpp`; match on the trailing filename via FILTER.
    #
    # AvgWeighted and SumMap stay always-built: the `_time_weighted` combinator
    # in AggregateFunctionFactory.cpp lowers `avg_time_weighted` to
    # `avg_weighted`, and SummingSortedAlgorithm dispatches to
    # `sum_map_with_overflow` for SummingMergeTree map merges. Excluding them
    # caused those internal paths to fail at runtime in mini.
    list(FILTER clickhouse_aggregate_functions_sources EXCLUDE REGEX
            "/AggregateFunction(Xirr|DeltaSumTimestamp|Sparkbar|SimpleLinearRegression|Covar|Corr|GroupArrayMoving)\\.cpp$")
    message(STATUS "Extended aggregate functions are disabled")
endif()

extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
    IAggregateFunction.cpp
    AggregateFunctionFactory.cpp
    AggregateFunctionCombinatorFactory.cpp
    AggregateFunctionState.cpp
    AggregateFunctionCount.cpp
    parseAggregateFunctionParameters.cpp
)

# Include JavaScript UDA adapter into dbms only when V8 is enabled.
# The source itself is guarded with USE_V8, so no additional removal is necessary when disabled.
if (USE_V8)
    extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
        AggregateFunctionJavaScriptAdapter.cpp
    )
    extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
        AggregateFunctionJavaScriptAdapter.h
    )
else()
    # Avoid compiling the adapter TU when V8 is disabled
    list(REMOVE_ITEM clickhouse_aggregate_functions_sources AggregateFunctionJavaScriptAdapter.cpp)
    list(REMOVE_ITEM clickhouse_aggregate_functions_headers AggregateFunctionJavaScriptAdapter.h)
    set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/AggregateFunctionJavaScriptAdapter.cpp PROPERTIES HEADER_FILE_ONLY TRUE)
endif()
extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
    IAggregateFunction.h
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
