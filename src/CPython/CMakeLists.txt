

set(CPython_HEADERS)
file(GLOB CPython_HEADERS "*.h")

set(CPython_SOURCES)
file(GLOB CPython_SOURCES "*.cpp")

# message(STATUS "CPython_HEADERS: ${CPython_HEADERS}"
#                 "CPython_SOURCES: ${CPython_SOURCES}")

add_library(cpython_util ${CPython_HEADERS} ${CPython_SOURCES})

target_link_libraries(cpython_util PUBLIC clickhouse_common_io)

target_compile_options(cpython_util PRIVATE -w)