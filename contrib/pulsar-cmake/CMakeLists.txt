# Needed when using Apache Pulsar external stream
option (ENABLE_PULSAR "Enable Pulsar" ${ENABLE_LIBRARIES})

if (NOT ENABLE_PULSAR)
    message(STATUS "Not using pulsar")
    return()
endif()

if (NOT ENABLE_CURL)
    message(FATAL_ERROR "Pulsar requires curl")
endif()

if (OS_DARWIN)
    message(FATAL_ERROR "Pulsar is not supported on MacOS yet")
endif()

set(PULSAR_CLIENT_CPP_DIR "${CMAKE_CURRENT_LIST_DIR}/pulsar-client-cpp")
set(PROTOC_PATH protoc)

execute_process(COMMAND cat ${PULSAR_CLIENT_CPP_DIR}/version.txt OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE PULSAR_CLIENT_VERSION)
message(STATUS "Pulsar Client version: ${PULSAR_CLIENT_VERSION}")

execute_process(COMMAND ${PULSAR_CLIENT_CPP_DIR}/build-support/gen-pulsar-version-macro.py OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE PULSAR_CLIENT_VERSION_MACRO)
message(STATUS "Pulsar Client version macro: ${PULSAR_CLIENT_VERSION_MACRO}")

set(PVM_COMMENT "This is generated from Version.h.in by CMAKE. DO NOT EDIT DIRECTLY")
configure_file(${PULSAR_CLIENT_CPP_DIR}/templates/Version.h.in ${PULSAR_CLIENT_CPP_DIR}/include/pulsar/Version.h @ONLY)

set(LIB_AUTOGEN_DIR ${CMAKE_CURRENT_LIST_DIR}/generated/lib)
set(PROTO_SOURCES
    ${LIB_AUTOGEN_DIR}/PulsarApi.pb.cc
    ${LIB_AUTOGEN_DIR}/PulsarApi.pb.h)

file(GLOB PULSAR_SOURCES
    ${PULSAR_CLIENT_CPP_DIR}/lib/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/lz4/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/lz4/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/checksum/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/checksum/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/stats/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/stats/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/c/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/c/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/auth/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/auth/*.h
    ${PULSAR_CLIENT_CPP_DIR}/lib/auth/athenz/*.cc
    ${PULSAR_CLIENT_CPP_DIR}/lib/auth/athenz/*.h)

set(PULSAR_SOURCES ${PULSAR_SOURCES} ${PROTO_SOURCES})

add_library(_pulsar ${PULSAR_SOURCES})
add_library(ch_contrib::pulsar ALIAS _pulsar)
set_target_properties (_pulsar PROPERTIES VERSION ${PULSAR_CLIENT_VERSION})

target_include_directories(_pulsar SYSTEM PUBLIC "${PULSAR_CLIENT_CPP_DIR}/include")
target_include_directories(_pulsar SYSTEM PUBLIC "${PULSAR_CLIENT_CPP_DIR}")
target_include_directories(_pulsar SYSTEM PUBLIC "${LIB_AUTOGEN_DIR}")

target_link_libraries(_pulsar PRIVATE boost::headers_only)
target_link_libraries(_pulsar PRIVATE ch_contrib::curl)
target_link_libraries(_pulsar
  PRIVATE
    ch_contrib::lz4
    ch_contrib::zlib
    ch_contrib::zstd
    OpenSSL::Crypto
    OpenSSL::SSL
    ch_contrib::protobuf
    ch_contrib::snappy
)
