include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(checkpoint .)

add_library(checkpoint ${checkpoint_headers} ${checkpoint_sources})

set(FLATBUFFERS_SRC_DIR "${proton_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

target_include_directories(checkpoint SYSTEM PUBLIC ${FLATBUFFERS_INCLUDE_DIR})

target_link_libraries(checkpoint PUBLIC clickhouse_common_io ch_contrib::abseil_swiss_tables ch_contrib::uv ch_contrib::rocksdb)

if (TARGET ch_contrib::ulid)
    target_link_libraries(checkpoint PUBLIC ch_contrib::ulid)
endif ()

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
