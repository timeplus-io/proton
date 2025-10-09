include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_common .)

add_headers_and_sources(cluster_common ./TimeWheel)

add_library(cluster_common ${cluster_common_headers} ${cluster_common_sources})

target_link_libraries(cluster_common PUBLIC clickhouse_common_io ch_contrib::abseil_swiss_tables)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif ()
