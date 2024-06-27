include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(external_stream .)

if (OS_LINUX)
    add_headers_and_sources(external_stream Log)
else()
    message(STATUS "Log stream is only supported on Linux for now")
endif()

add_headers_and_sources(external_stream Kafka)
add_headers_and_sources(external_stream Timeplus)

add_library(external_stream ${external_stream_headers} ${external_stream_sources})

target_link_libraries(external_stream PUBLIC klog)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
