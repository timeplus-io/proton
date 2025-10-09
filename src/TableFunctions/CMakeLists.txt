include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_table_functions .)

extract_into_parent_list(clickhouse_table_functions_sources dbms_sources
    ITableFunction.cpp
    TableFunctionFactory.cpp
)
extract_into_parent_list(clickhouse_table_functions_headers dbms_headers
    ITableFunction.h
    TableFunctionFactory.h
)

add_library(clickhouse_table_functions ${clickhouse_table_functions_headers} ${clickhouse_table_functions_sources})
target_link_libraries(clickhouse_table_functions PRIVATE clickhouse_parsers clickhouse_storages_system dbms)

