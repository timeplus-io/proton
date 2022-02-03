add_headers_and_sources(nlog Base)
add_headers_and_sources(nlog Common)
add_headers_and_sources(nlog Checkpoints)
add_headers_and_sources(nlog Log)
add_headers_and_sources(nlog MetaStore)
add_headers_and_sources(nlog Cluster)
add_headers_and_sources(nlog Sequencer)
add_headers_and_sources(nlog Server)

add_subdirectory(Common)

add_library(nlog ${nlog_headers} ${nlog_sources})

target_include_directories(nlog SYSTEM PUBLIC
        "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers/include"
        "${ROCKSDB_INCLUDE_DIR}")

target_link_libraries(nlog PUBLIC clickhouse_common_io ch_contrib::rocksdb)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
