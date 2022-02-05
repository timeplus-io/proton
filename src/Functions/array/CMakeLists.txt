include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_array .)

if (NOT ENABLE_HIGH_ORDER_ARRAY_FUNCS)
    SET(HIGH_ORDER_FUNCS
            arrayMap.cpp
            arrayFilter.cpp
            arrayCount.cpp
            arrayExists.cpp
            arrayAll.cpp
            arrayCompact.cpp
            arrayAggregation.cpp
            arrayFirst.cpp
            arrayFirstLastIndex.cpp
            arrayFill.cpp
            arraySplit.cpp
            arraySort.cpp
            arrayCumSum.cpp
            arrayCumSumNonNegative.cpp
            arrayDifference.cpp
    )

    list(REMOVE_ITEM clickhouse_functions_array
           ${HIGH_ORDER_FUNCS}
    )
endif()

add_library(clickhouse_functions_array ${clickhouse_functions_array_sources} ${clickhouse_functions_array_headers})
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_array PRIVATE "-g0")
endif()
