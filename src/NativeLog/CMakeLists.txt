include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(nlog Base)
add_headers_and_sources(nlog Rocks)
add_headers_and_sources(nlog Record)
add_headers_and_sources(nlog Requests)
add_headers_and_sources(nlog Common)
add_headers_and_sources(nlog Cache)
add_headers_and_sources(nlog Checkpoints)
add_headers_and_sources(nlog Log)
add_headers_and_sources(nlog MetaStore)
add_headers_and_sources(nlog Server)

add_subdirectory(Common)

add_library(nlog ${nlog_headers} ${nlog_sources})

target_include_directories(nlog SYSTEM PUBLIC
        # "${proton_SOURCE_DIR}/contrib/flatbuffers/include"
        "${ROCKSDB_INCLUDE_DIR}")

target_link_libraries(nlog PUBLIC clickhouse_common_io ch_contrib::rocksdb ch_contrib::abseil_swiss_tables)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()
