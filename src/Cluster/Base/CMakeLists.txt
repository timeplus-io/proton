include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_base .)

add_library(cluster_base ${cluster_base_headers} ${cluster_base_sources})

set(FLATBUFFERS_SRC_DIR "${proton_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

target_include_directories(cluster_base SYSTEM PUBLIC ${FLATBUFFERS_INCLUDE_DIR})
target_link_libraries(cluster_base PUBLIC clickhouse_common_io ch_contrib::abseil_swiss_tables)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
