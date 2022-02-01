SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing")
INCLUDE_DIRECTORIES(BEFORE ${LIBRARY_DIR} "${LIBRARY_DIR}/src/include")

configure_file(compat.h "${LIBRARY_DIR}/src/include/liburing")

set (SRCS
    "${LIBRARY_DIR}/src/syscall.h"
    "${LIBRARY_DIR}/src/syscall.c"
    "${LIBRARY_DIR}/src/arch/x86/syscall.h"
    "${LIBRARY_DIR}/src/arch/x86/lib.h"
    "${LIBRARY_DIR}/src/liburing.map"
    "${LIBRARY_DIR}/src/queue.c"
    "${LIBRARY_DIR}/src/include/liburing.h"
    "${LIBRARY_DIR}/src/include/liburing/barrier.h"
    "${LIBRARY_DIR}/src/include/liburing/io_uring.h"
    "${LIBRARY_DIR}/src/include/liburing/compat.h"
    # "${LIBRARY_DIR}/src/nolibc.c"
    "${LIBRARY_DIR}/src/register.c"
    "${LIBRARY_DIR}/src/setup.c"
    "${LIBRARY_DIR}/src/lib.h")

add_library(${LIBURING_LIBRARY} ${SRCS})

target_include_directories(${LIBURING_LIBRARY}
    PUBLIC
        "${LIBRARY_DIR}/src/include")
