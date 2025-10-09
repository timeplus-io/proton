include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(nlog Base)
add_headers_and_sources(nlog Common)
add_headers_and_sources(nlog Indexes)
add_headers_and_sources(nlog Log)

add_subdirectory(Common)
add_subdirectory(Log)

add_library(nlog ${nlog_headers} ${nlog_sources})

set(FLATBUFFERS_SRC_DIR "${proton_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

target_include_directories(nlog SYSTEM PUBLIC ${FLATBUFFERS_INCLUDE_DIR})

target_link_libraries(nlog PUBLIC cluster_requests cluster_rocks ch_contrib::abseil_swiss_tables)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (ENABLE_BENCHMARKS)
    add_subdirectory(Indexes/benchmarks)
endif ()
