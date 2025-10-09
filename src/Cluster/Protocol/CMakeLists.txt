include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_protocol .)

add_library(cluster_protocol ${cluster_protocol_headers} ${cluster_protocol_sources})

target_link_libraries(cluster_protocol PUBLIC cluster_common)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
