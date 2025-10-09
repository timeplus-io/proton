include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_rocks .)

add_library(cluster_rocks ${cluster_rocks_headers} ${cluster_rocks_sources})

target_include_directories(cluster_rocks SYSTEM PUBLIC
        # "${proton_SOURCE_DIR}/contrib/flatbuffers/include"
        "${ROCKSDB_INCLUDE_DIR}")

target_link_libraries(cluster_rocks PUBLIC ch_contrib::rocksdb clickhouse_common_io)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()


