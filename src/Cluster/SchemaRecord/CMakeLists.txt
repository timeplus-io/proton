include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_schema_record .)

add_library(cluster_schema_record ${cluster_schema_record_headers} ${cluster_schema_record_sources})

target_link_libraries(cluster_schema_record PUBLIC clickhouse_common_io)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()
