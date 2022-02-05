option (ENABLE_URING "Enable uring" ${ENABLE_LIBRARIES})

if (NOT ENABLE_URING)
    message(STATUS "Not using liburing")
    return()
endif()

SET(LIBRARY_DIR "${proton_SOURCE_DIR}/contrib/liburing")
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

add_library(_uring ${SRCS})
add_library(ch_contrib::uring ALIAS _uring)

target_include_directories(_uring
    PUBLIC
        "${LIBRARY_DIR}/src/include")
