add_executable (streaming_records streaming_records_example.cpp)
target_link_libraries (streaming_records PRIVATE clickhouse_common_io)

# FIXME
set(FLATBUFFERS_SRC_DIR "${proton_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

target_include_directories(streaming_records SYSTEM PRIVATE ${FLATBUFFERS_INCLUDE_DIR})
