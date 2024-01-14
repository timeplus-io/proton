include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(external_table .)

add_library(external_table ${external_table_headers} ${external_table_sources})

target_link_libraries(external_table PUBLIC clickhouse_parsers)

# if (ENABLE_TESTS)
#     add_subdirectory(tests)
# endif ()
