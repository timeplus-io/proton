include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(checkpoint .)

add_library(checkpoint ${checkpoint_headers} ${checkpoint_sources})

target_link_libraries(checkpoint PUBLIC clickhouse_common_io ch_contrib::abseil_swiss_tables)
