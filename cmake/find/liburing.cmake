option (ENABLE_LIBURING "Enable liburing" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBURING)
    return()
endif()

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/liburing/Makefile")
    message(WARNING "submodule contrib/liburing is missing. to fix try run: \n git submodule update --init --recursive")
    message (FATAL "Can't find internal liburing library")
endif()

set (LIBURING_LIBRARY uring)
set (USE_LIBURING 1)

set (LIBURING_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/liburing/src/include)

message (STATUS "Using liburing: ${LIBURING_INCLUDE_DIR} : ${LIBURING_LIBRARY}")
