
include(${proton_SOURCE_DIR}/contrib/protobuf-cmake/protobuf_generate.cmake)

# create_request_and_response_serde
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS create_stream_request.proto)

proton_add_executable(create_request_and_response_serde create_stream_request_ser_deser_perf.cpp ${PROTO_SRCS} ${PROTO_HDRS})
set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS}
                            PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier")

target_compile_options(create_request_and_response_serde PRIVATE -Wno-reserved-identifier)

target_include_directories(create_request_and_response_serde PRIVATE ${PROTOBUF_INCLUDE_DIRS})
target_include_directories(create_request_and_response_serde PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(create_request_and_response_serde PRIVATE ch_contrib::protobuf dbms)

