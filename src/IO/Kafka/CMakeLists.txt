include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(io_kafka .)

add_library(io_kafka ${io_kafka_headers} ${io_kafka_sources})

target_link_libraries(io_kafka PUBLIC clickhouse_common_io ch_contrib::rdkafka)

if (TARGET ch_contrib::aws_sdk)
    target_link_libraries(io_kafka PUBLIC ch_contrib::aws_sdk)
endif()

# if (ENABLE_TESTS)
#     add_subdirectory(tests)
# endif ()
