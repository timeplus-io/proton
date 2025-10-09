include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_requests .)

add_library(cluster_requests ${cluster_requests_headers} ${cluster_requests_sources})

target_link_libraries(cluster_requests PUBLIC cluster_protocol)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
