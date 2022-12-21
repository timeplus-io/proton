include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_streaming .)
add_library(clickhouse_functions_streaming ${clickhouse_functions_streaming_sources} ${clickhouse_functions_streaming_headers})
target_link_libraries(clickhouse_functions_streaming PRIVATE dbms)
