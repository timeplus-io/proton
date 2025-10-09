# This file is a modified version of contrib/libuv/CMakeLists.txt

set (SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/libuv")
set (BINARY_DIR "${PROJECT_BINARY_DIR}/contrib/libuv")

set(uv_sources
    src/fs-poll.c
    src/idna.c
    src/inet.c
    src/random.c
    src/strscpy.c
    src/strtok.c
    src/thread-common.c
    src/threadpool.c
    src/timer.c
    src/uv-common.c
    src/uv-data-getter-setters.c
    src/version.c)

if(WIN32)
    list(APPEND uv_defines WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0602)
    list(APPEND uv_libraries
            psapi
            user32
            advapi32
            iphlpapi
            userenv
            ws2_32
            dbghelp
            ole32
            uuid
            shell32)
    list(APPEND uv_sources
            src/win/async.c
            src/win/core.c
            src/win/detect-wakeup.c
            src/win/dl.c
            src/win/error.c
            src/win/fs.c
            src/win/fs-event.c
            src/win/getaddrinfo.c
            src/win/getnameinfo.c
            src/win/handle.c
            src/win/loop-watcher.c
            src/win/pipe.c
            src/win/thread.c
            src/win/poll.c
            src/win/process.c
            src/win/process-stdio.c
            src/win/signal.c
            src/win/snprintf.c
            src/win/stream.c
            src/win/tcp.c
            src/win/tty.c
            src/win/udp.c
            src/win/util.c
            src/win/winapi.c
            src/win/winsock.c)
    list(APPEND uv_test_libraries ws2_32)
    list(APPEND uv_test_sources src/win/snprintf.c test/runner-win.c)
else()
    list(APPEND uv_defines _FILE_OFFSET_BITS=64 _LARGEFILE_SOURCE)
    if(NOT CMAKE_SYSTEM_NAME MATCHES "Android|OS390|QNX")
        # TODO: This should be replaced with find_package(Threads) if possible
        # Android has pthread as part of its c library, not as a separate
        # libpthread.so.
        list(APPEND uv_libraries pthread)
    endif()
    list(APPEND uv_sources
            src/unix/async.c
            src/unix/core.c
            src/unix/dl.c
            src/unix/fs.c
            src/unix/getaddrinfo.c
            src/unix/getnameinfo.c
            src/unix/loop-watcher.c
            src/unix/loop.c
            src/unix/pipe.c
            src/unix/poll.c
            src/unix/process.c
            src/unix/random-devurandom.c
            src/unix/signal.c
            src/unix/stream.c
            src/unix/tcp.c
            src/unix/thread.c
            src/unix/tty.c
            src/unix/udp.c)
    list(APPEND uv_test_sources test/runner-unix.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "AIX")
    list(APPEND uv_defines
        _ALL_SOURCE
        _LINUX_SOURCE_COMPAT
        _THREAD_SAFE
        _XOPEN_SOURCE=500
        HAVE_SYS_AHAFS_EVPRODS_H)

    list(APPEND uv_libraries perfstat)
    list(APPEND uv_sources
        src/unix/aix.c
        src/unix/aix-common.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Android")
    list(APPEND uv_defines _GNU_SOURCE)
    list(APPEND uv_libraries dl)
    list(APPEND uv_sources
            src/unix/linux.c
            src/unix/procfs-exepath.c
            src/unix/random-getentropy.c
            src/unix/random-getrandom.c
            src/unix/random-sysctl-linux.c)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME MATCHES "Android|Linux")
    list(APPEND uv_sources src/unix/proctitle.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD")
  list(APPEND uv_sources src/unix/freebsd.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD|NetBSD|OpenBSD")
  list(APPEND uv_sources src/unix/posix-hrtime.c src/unix/bsd-proctitle.c)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD|NetBSD|OpenBSD")
  list(APPEND uv_sources src/unix/bsd-ifaddrs.c src/unix/kqueue.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  list(APPEND uv_sources src/unix/random-getrandom.c)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
  list(APPEND uv_sources src/unix/random-getentropy.c)
endif()

if(APPLE)
  list(APPEND uv_defines _DARWIN_UNLIMITED_SELECT=1 _DARWIN_USE_64_BIT_INODE=1)
  list(APPEND uv_sources
       src/unix/darwin-proctitle.c
       src/unix/darwin.c
       src/unix/fsevents.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "GNU")
  list(APPEND uv_libraries dl)
  list(APPEND uv_sources
       src/unix/bsd-ifaddrs.c
       src/unix/no-fsevents.c
       src/unix/no-proctitle.c
       src/unix/posix-hrtime.c
       src/unix/posix-poll.c
       src/unix/hurd.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND uv_defines _GNU_SOURCE _POSIX_C_SOURCE=200112)
  list(APPEND uv_libraries rt)
  list(APPEND uv_sources
       src/unix/linux.c
       src/unix/procfs-exepath.c
       src/unix/random-getrandom.c
       src/unix/random-sysctl-linux.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "NetBSD")
  list(APPEND uv_sources src/unix/netbsd.c)
  list(APPEND uv_libraries kvm)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
  list(APPEND uv_sources src/unix/openbsd.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "OS/390")
  list(APPEND uv_defines PATH_MAX=1024)
  list(APPEND uv_defines _AE_BIMODAL)
  list(APPEND uv_defines _ALL_SOURCE)
  list(APPEND uv_defines _ENHANCED_ASCII_EXT=0xFFFFFFFF)
  list(APPEND uv_defines _ISOC99_SOURCE)
  list(APPEND uv_defines _LARGE_TIME_API)
  list(APPEND uv_defines _OPEN_MSGQ_EXT)
  list(APPEND uv_defines _OPEN_SYS_FILE_EXT)
  list(APPEND uv_defines _OPEN_SYS_IF_EXT)
  list(APPEND uv_defines _OPEN_SYS_SOCK_EXT3)
  list(APPEND uv_defines _OPEN_SYS_SOCK_IPV6)
  list(APPEND uv_defines _UNIX03_SOURCE)
  list(APPEND uv_defines _UNIX03_THREADS)
  list(APPEND uv_defines _UNIX03_WITHDRAWN)
  list(APPEND uv_defines _XOPEN_SOURCE=600)
  list(APPEND uv_defines _XOPEN_SOURCE_EXTENDED)
  list(APPEND uv_sources
       src/unix/os390.c
       src/unix/os390-syscalls.c
       src/unix/os390-proctitle.c)

  list(APPEND uv_cflags
       -q64
       -qascii
       -qexportall
       -qgonumber
       -qlongname
       -qlibansi
       -qfloat=IEEE
       -qtune=10
       -qarch=10
       -qasm
       -qasmlib=sys1.maclib:sys1.modgen)

    find_library(ZOSLIB
      NAMES zoslib
      PATHS ${ZOSLIB_DIR}
      PATH_SUFFIXES lib
    )
    list(APPEND uv_libraries ${ZOSLIB})
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "OS400")
    list(APPEND uv_defines
        _ALL_SOURCE
        _LINUX_SOURCE_COMPAT
        _THREAD_SAFE
        _XOPEN_SOURCE=500)

    list(APPEND uv_sources
        src/unix/aix-common.c
        src/unix/ibmi.c
        src/unix/no-fsevents.c
        src/unix/posix-poll.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    if(CMAKE_SYSTEM_VERSION STREQUAL "5.10")
        list(APPEND uv_defines SUNOS_NO_IFADDRS)
        list(APPEND uv_libraries rt)
    endif()

    list(APPEND uv_defines __EXTENSIONS__ _XOPEN_SOURCE=500 _REENTRANT)
    list(APPEND uv_libraries kstat nsl sendfile socket)
    list(APPEND uv_sources
         src/unix/no-proctitle.c
         src/unix/sunos.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Haiku")
    list(APPEND uv_defines _BSD_SOURCE)
    list(APPEND uv_libraries bsd network)
    list(APPEND uv_sources
        src/unix/haiku.c
        src/unix/bsd-ifaddrs.c
        src/unix/no-fsevents.c
        src/unix/no-proctitle.c
        src/unix/posix-hrtime.c
        src/unix/posix-poll.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "QNX")
    list(APPEND uv_sources
        src/unix/posix-hrtime.c
        src/unix/posix-poll.c
        src/unix/qnx.c
        src/unix/bsd-ifaddrs.c
        src/unix/no-proctitle.c
        src/unix/no-fsevents.c)
    list(APPEND uv_libraries socket)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD|Linux|NetBSD|OpenBSD")
    list(APPEND uv_test_libraries util)
endif()

if(CYGWIN OR MSYS)
    list(APPEND uv_defines _GNU_SOURCE)
    list(APPEND uv_sources
        src/unix/cygwin.c
        src/unix/bsd-ifaddrs.c
        src/unix/no-fsevents.c
        src/unix/no-proctitle.c
        src/unix/posix-hrtime.c
        src/unix/posix-poll.c
        src/unix/procfs-exepath.c
        src/unix/sysinfo-loadavg.c
        src/unix/sysinfo-memory.c)
endif()

set(uv_sources_tmp "")
foreach(file ${uv_sources})
  list(APPEND uv_sources_tmp "${SOURCE_DIR}/${file}")
endforeach(file)
set(uv_sources "${uv_sources_tmp}")

list(APPEND uv_defines CLICKHOUSE_GLIBC_COMPATIBILITY)

add_library(_uv ${uv_sources})
add_library(ch_contrib::uv ALIAS _uv)

target_compile_definitions(_uv PRIVATE ${uv_defines})
target_include_directories(_uv SYSTEM PUBLIC ${SOURCE_DIR}/include PRIVATE ${SOURCE_DIR}/src)
target_link_libraries(_uv ${uv_libraries})

if(UNIX)
  # Now for some gibbering horrors from beyond the stars...
  foreach(x ${uv_libraries})
    set(LIBS "${LIBS} -l${x}")
  endforeach(x)
  file(STRINGS ${SOURCE_DIR}/configure.ac configure_ac REGEX ^AC_INIT)
  string(REGEX MATCH [0-9]+[.][0-9]+[.][0-9]+ PACKAGE_VERSION "${configure_ac}")
  string(REGEX MATCH ^[0-9]+ UV_VERSION_MAJOR "${PACKAGE_VERSION}")
  # The version in the filename is mirroring the behaviour of autotools.
  set_target_properties(_uv PROPERTIES VERSION ${UV_VERSION_MAJOR}.0.0
                        SOVERSION ${UV_VERSION_MAJOR})
endif()
