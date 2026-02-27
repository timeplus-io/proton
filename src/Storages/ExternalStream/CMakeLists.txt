include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(external_stream .)

add_headers_and_sources(external_stream Log)
add_headers_and_sources(external_stream Kafka)
add_headers_and_sources(external_stream Pulsar)
add_headers_and_sources(external_stream Timeplus)
add_headers_and_sources(external_stream S3)
add_headers_and_sources(external_stream Iceberg)
add_headers_and_sources(external_stream HTTP)
add_headers_and_sources(external_stream Python)
add_headers_and_sources(external_stream NATSJetstream)

add_library(external_stream ${external_stream_headers} ${external_stream_sources})

target_link_libraries(external_stream PUBLIC klog)

if (TARGET ch_contrib::rdkafka)
    target_link_libraries(external_stream PRIVATE io_kafka)
endif()

if (TARGET ch_contrib::pulsar)
    target_link_libraries(external_stream PRIVATE ch_contrib::pulsar)
endif()

if (TARGET ch_contrib::avrocpp)
    target_link_libraries(external_stream PRIVATE ch_contrib::avrocpp)
endif()

if (TARGET ch_contrib::parquet)
    target_link_libraries(external_stream PRIVATE ch_contrib::parquet)
endif()

if (TARGET ch_contrib::nats_io)
    target_link_libraries(external_stream PRIVATE ch_contrib::nats_io)
endif()

target_link_libraries(external_stream PRIVATE ch_contrib::uv)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
