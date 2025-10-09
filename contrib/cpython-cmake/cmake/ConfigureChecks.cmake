include(CheckCSourceCompiles)
include(CheckCSourceRuns)
include(CheckIncludeFiles)
include(CheckTypeSize)
include(CMakePushCheckState)
include(CheckStructHasMember)
include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckVariableExists)
include(cmake/PlatformTest.cmake)
include(TestBigEndian)

message(STATUS "The system name is ${CMAKE_SYSTEM_NAME}")
message(STATUS "The system processor is ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "The system version is ${CMAKE_SYSTEM_VERSION}")

# FIXME: build bzip from source code
# # Find any dependencies
# if(USE_SYSTEM_BZip2)
#     find_package(BZip2) # https://cmake.org/cmake/help/latest/module/FindBZip2.html
# endif()
# message(STATUS "BZIP2_INCLUDE_DIR=${BZIP2_INCLUDE_DIR}")
# message(STATUS "BZIP2_LIBRARIES=${BZIP2_LIBRARIES}")

# FIXME: build curses from source code
# if(USE_SYSTEM_Curses)
#     find_package(Curses) # https://cmake.org/cmake/help/latest/module/FindCurses.html
#     find_library(PANEL_LIBRARY NAMES panel)
#     set(PANEL_LIBRARIES ${PANEL_LIBRARY})
#     if(WITH_STATIC_DEPENDENCIES)
#         find_library(TINFO_LIBRARY NAMES tinfo)
#         find_library(GPM_LIBRARY NAMES gpm)
#     endif()
# endif()
# message(STATUS "CURSES_LIBRARIES=${CURSES_LIBRARIES}")
# message(STATUS "PANEL_LIBRARIES=${PANEL_LIBRARIES}")
# if(WITH_STATIC_DEPENDENCIES)
#     message(STATUS "TINFO_LIBRARY=${TINFO_LIBRARY}")
#     message(STATUS "GPM_LIBRARY=${GPM_LIBRARY}")
# endif()

# FIXME: build EXPAT from source code
# if(USE_SYSTEM_EXPAT)
#     find_package(EXPAT) # https://cmake.org/cmake/help/latest/module/FindEXPAT.html
# endif()
# message(STATUS "EXPAT_LIBRARIES=${EXPAT_LIBRARIES}")
# message(STATUS "EXPAT_INCLUDE_DIRS=${EXPAT_INCLUDE_DIRS}")

# FIXME: build libffi from source code
# if(USE_SYSTEM_LibFFI)
#     find_path(LibFFI_INCLUDE_DIR ffi.h)
#     find_library(LibFFI_LIBRARY NAMES ffi libffi)
#     message(STATUS "LibFFI_INCLUDE_DIR=${LibFFI_INCLUDE_DIR}")
#     message(STATUS "LibFFI_LIBRARY=${LibFFI_LIBRARY}")
# endif()

# FIXME: build libmpdec from source code
# if(IS_PY3 AND USE_SYSTEM_LIBMPDEC)
#     find_library(LIBMPDEC_LIBRARY NAMES mpdec libmpdec)
#     set(LIBMPDEC_LIBRARIES ${LIBMPDEC_LIBRARY})
#     message(STATUS "LIBMPDEC_LIBRARIES=${LIBMPDEC_LIBRARIES}")
# endif()

# FIXME: build openssl from source code
# if(USE_SYSTEM_OpenSSL)
#     find_package(OpenSSL 0.9.7) # https://cmake.org/cmake/help/latest/module/FindOpenSSL.html
#     message(STATUS "OPENSSL_LIBRARIES=${OPENSSL_LIBRARIES}")
#     message(STATUS "OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}")
# endif()

# FIXME: build TCL from source code
# if(USE_SYSTEM_TCL)
#     find_package(TCL)
# endif()

# FIXME: build X11 from source code
# if(UNIX)
#     # Only needed by _tkinter
#     find_package(X11)
# endif()

# FIXME: build zlib from source code
# if(USE_SYSTEM_ZLIB)
#     find_package(ZLIB) # https://cmake.org/cmake/help/latest/module/FindZLIB.html
#     message(STATUS "ZLIB_INCLUDE_DIRS=${ZLIB_INCLUDE_DIRS}")
#     message(STATUS "ZLIB_LIBRARIES=${ZLIB_LIBRARIES}")
# endif()

# FIXME: build db from source code
# if(USE_SYSTEM_DB)
#     find_path(DB_INCLUDE_PATH db.h)
#     find_library(DB_LIBRARY NAMES db-4.8)
#     message(STATUS "DB_INCLUDE_PATH=${DB_INCLUDE_PATH}")
#     message(STATUS "DB_LIBRARY=${DB_LIBRARY}")
# endif()

# FIXME: build gdbm from source code
# if(USE_SYSTEM_GDBM)
#     find_path(GDBM_INCLUDE_PATH gdbm.h)
#     find_library(GDBM_LIBRARY gdbm)
#     find_library(GDBM_COMPAT_LIBRARY gdbm_compat)
#     find_path(NDBM_INCLUDE_PATH ndbm.h)

#     if(NDBM_INCLUDE_PATH)
#         set(NDBM_TAG NDBM)
#     else()
#         find_path(GDBM_NDBM_INCLUDE_PATH gdbm/ndbm.h)
#         if(GDBM_NDBM_INCLUDE_PATH)
#             set(NDBM_TAG GDBM_NDBM)
#         else()
#             find_path(GDBM_DASH_NDBM_INCLUDE_PATH gdbm-ndbm.h)
#             if(GDBM_DASH_NDBM_INCLUDE_PATH)
#                 set(NDBM_TAG GDBM_DASH_NDBM)
#             endif()
#         endif()
#     endif()
# endif()
# message(STATUS "GDBM_INCLUDE_PATH=${GDBM_INCLUDE_PATH}")
# message(STATUS "GDBM_LIBRARY=${GDBM_LIBRARY}")
# message(STATUS "GDBM_COMPAT_LIBRARY=${GDBM_COMPAT_LIBRARY}")
# message(STATUS "NDBM_TAG=${NDBM_TAG}")
# message(STATUS "<NDBM_TAG>_INCLUDE_PATH=${${NDBM_TAG}_INCLUDE_PATH}")

# FIXME: build lzma from source code
# find_path(LZMA_INCLUDE_PATH lzma.h)
# find_library(LZMA_LIBRARY lzma)
# message(STATUS "LZMA_INCLUDE_PATH=${LZMA_INCLUDE_PATH}")
# message(STATUS "LZMA_LIBRARY=${LZMA_LIBRARY}")

# FIXME: build readline from source code
# if(USE_SYSTEM_READLINE)
#     if(USE_LIBEDIT)
#         find_path(READLINE_INCLUDE_PATH editline/readline.h)
#         find_library(READLINE_LIBRARY edit)
#     else()
#         find_path(READLINE_INCLUDE_PATH readline/readline.h)
#         find_library(READLINE_LIBRARY readline)
#     endif()
# endif()
# message(STATUS "READLINE_INCLUDE_PATH=${READLINE_INCLUDE_PATH}")
# message(STATUS "READLINE_LIBRARY=${READLINE_LIBRARY}")

# FIXME: build sqlite3 from source code
# if(DEFINED SQLITE3_INCLUDE_PATH)
#     message(AUTHOR_WARNING "Setting SQLITE3_INCLUDE_PATH is deprecated. SQLite3_INCLUDE_DIR should be set instead.")
#     set(SQLite3_INCLUDE_DIR ${SQLITE3_INCLUDE_PATH})
# endif()
# if(DEFINED SQLITE3_LIBRARY)
#     message(AUTHOR_WARNING "Setting SQLITE3_LIBRARY is deprecated. SQLite3_LIBRARY should be set instead.")
#     set(SQLite3_LIBRARY ${SQLITE3_LIBRARY})
# endif()
# if(USE_SYSTEM_SQLite3)
#     if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.14")
#     find_package(SQLite3) # https://cmake.org/cmake/help/latest/module/FindSQLite3.html
#     else()
#     find_path(SQLite3_INCLUDE_DIR sqlite3.h)
#     find_library(SQLite3_LIBRARY sqlite3)
#     endif()
# endif()
# set(SQLite3_INCLUDE_DIRS ${SQLite3_INCLUDE_DIR})
# set(SQLite3_LIBRARIES ${SQLite3_LIBRARY})
# message(STATUS "SQLite3_INCLUDE_DIRS=${SQLite3_INCLUDE_DIRS}")
# message(STATUS "SQLite3_LIBRARIES=${SQLite3_LIBRARIES}")

# FIXME: build tirpc from source code
# find_path(TIRPC_RPC_INCLUDE_PATH rpc.h PATHS "/usr/include/tirpc/rpc")
# find_library(TIRPC_LIBRARY tirpc)

# FIXME: build libuuid from source code
# find_library(UUID_LIBRARY uuid)

# FIXME: choose hash algorithm
# if(IS_PY3)
# set(_msg "Checking WITH_HASH_ALGORITHM option")
# message(STATUS "${_msg}")
# if(WITH_HASH_ALGORITHM STREQUAL "default")
#   set(Py_HASH_ALGORITHM 0)
# elseif(WITH_HASH_ALGORITHM STREQUAL "siphash24")
#   set(Py_HASH_ALGORITHM 1)
# elseif(WITH_HASH_ALGORITHM STREQUAL "fnv")
#   set(Py_HASH_ALGORITHM 2)
# else()
#   message(FATAL_ERROR "Unknown hash algorithm '${Py_HASH_ALGORITHM}'")
# endif()
# message(STATUS "${_msg} [${WITH_HASH_ALGORITHM}]")

# ABI version string for Python extension modules.  This appears between the
# periods in shared library file names, e.g. foo.<SOABI>.so.  It is calculated
# from the following attributes which affect the ABI of this Python build (in
# this order):
#
# * The Python implementation (always 'cpython-' for us)
# * The major and minor version numbers
# * --with-pydebug (adds a 'd')
#
# Thus for example, Python 3.2 built with wide unicode, pydebug, and pymalloc,
# would get a shared library ABI version tag of 'cpython-32dmu' and shared
# libraries would be named 'foo.cpython-32dmu.so'.
#
# In Python 3.2 and older, --with-wide-unicode added a 'u' flag.
# In Python 3.7 and older, --with-pymalloc added a 'm' flag.

set(_msg "Checking ABIFLAGS")
set(ABIFLAGS )
# if(Py_DEBUG)
#   set(ABIFLAGS "${ABIFLAGS}d")
# endif()
# if(WITH_PYMALLOC AND PY_VERSION VERSION_LESS "3.8")
#   set(ABIFLAGS "${ABIFLAGS}m")
# endif()
message(STATUS "${_msg} - ${ABIFLAGS}")

# FIXME: checking SOABI
set(_msg "Checking SOABI")

if (OS_LINUX)
  if (ARCH_AMD64)
    set(PLATFORM_TRIPLET "x86_64-linux-gnu")
  elseif (ARCH_AARCH64)
    set(PLATFORM_TRIPLET "aarch64-linux-gnu")
  endif ()
elseif (OS_DARWIN)
  set(PLATFORM_TRIPLET "darwin")
endif ()

if (NOT PLATFORM_TRIPLET)
  message(FATAL_ERROR "Doesn't support python UDF on platform: ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR}, please set ENABLE_PYTHON_UDF OFF")
endif ()

set(SOABI "cpython-${PY_VERSION_MAJOR}${PY_VERSION_MINOR}${ABIFLAGS}-${PLATFORM_TRIPLET}")

message(STATUS "${_msg} - ${SOABI}")

# endif()

# FIXME: check ALT_SOABI
# if(PY_VERSION VERSION_GREATER_EQUAL "3.8")

# # Alternative SOABI used in debug build to load C extensions built in release mode
# # Release and debug (Py_DEBUG) ABI are compatible, but not Py_TRACE_REFS ABI
# if(Py_DEBUG AND NOT WITH_TRACE_REFS)
#   string(REPLACE "d" "" ALT_ABIFLAGS "${ABIFLAGS}")
#   set(ALT_SOABI "cpython-${PY_VERSION_MAJOR}${PY_VERSION_MINOR}${ALT_ABIFLAGS}-${PLATFORM_TRIPLET}")
# endif()

# endif()

macro(ADD_COND var cond item)
  if(${cond})
    set(${var} ${${var}} ${item})
  endif()
endmacro()

set(CMAKE_REQUIRED_DEFINITIONS )

# Convenient macro allowing to conditonally update CMAKE_REQUIRED_DEFINITIONS
macro(set_required_def var value)
  set(${var} ${value})
  list(APPEND CMAKE_REQUIRED_DEFINITIONS "-D${var}=${value}")
endmacro()

# Emulate AC_HEADER_DIRENT
check_include_files(dirent.h HAVE_DIRENT_H)
if(NOT HAVE_DIRENT_H)
  check_include_files(sys/ndir.h HAVE_SYS_NDIR_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "sys/dir.h")
  check_type_size(DIR HAVE_SYS_DIR_H)
  check_include_files(ndir.h HAVE_NDIR_H)
endif()
if(IS_PY3)
  check_symbol_exists("dirfd" "sys/types.h;dirent.h" HAVE_DIRFD)
endif()

check_include_files(alloca.h HAVE_ALLOCA_H) # libffi and cpython
check_include_files(asm/types.h HAVE_ASM_TYPES_H)
check_include_files(arpa/inet.h HAVE_ARPA_INET_H)
check_include_files(bluetooth/bluetooth.h HAVE_BLUETOOTH_BLUETOOTH_H)
check_include_files(bluetooth.h HAVE_BLUETOOTH_H)
check_include_files(conio.h HAVE_CONIO_H)
check_include_files(crypt.h HAVE_CRYPT_H)
check_include_files(curses.h HAVE_CURSES_H)
check_include_files(direct.h HAVE_DIRECT_H)
check_include_files(dlfcn.h HAVE_DLFCN_H) # libffi and cpython
check_include_files(errno.h HAVE_ERRNO_H)
check_include_files(fcntl.h HAVE_FCNTL_H)
check_include_files(fpu_control.h HAVE_FPU_CONTROL_H)
check_include_files(grp.h HAVE_GRP_H)
check_include_files(ieeefp.h HAVE_IEEEFP_H)
check_include_files(inttypes.h HAVE_INTTYPES_H) # libffi and cpython
check_include_files(io.h HAVE_IO_H)
check_include_files(langinfo.h HAVE_LANGINFO_H)
check_include_files(libintl.h HAVE_LIBINTL_H)
check_include_files(libutil.h HAVE_LIBUTIL_H)
check_include_files(linux/tipc.h HAVE_LINUX_TIPC_H)
check_include_files(locale.h HAVE_LOCALE_H)

check_include_files(sys/socket.h HAVE_SYS_SOCKET_H)

set(LINUX_NETLINK_HEADERS)
add_cond(LINUX_NETLINK_HEADERS HAVE_ASM_TYPES_H  asm/types.h)
add_cond(LINUX_NETLINK_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
set(LINUX_NETLINK_HEADERS ${LINUX_NETLINK_HEADERS} linux/netlink.h)
check_include_files("${LINUX_NETLINK_HEADERS}" HAVE_LINUX_NETLINK_H)


if(IS_PY3)
  set(LINUX_QRTR_HEADERS)
  add_cond(LINUX_QRTR_HEADERS HAVE_ASM_TYPES_H  asm/types.h)
  add_cond(LINUX_QRTR_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
  set(LINUX_QRTR_HEADERS ${LINUX_QRTR_HEADERS} linux/qrtr.h)
  check_include_files("${LINUX_QRTR_HEADERS}" HAVE_LINUX_QRTR_H)

  # On Linux, can.h and can/raw.h require sys/socket.h
  set(LINUX_CAN_HEADERS)
  add_cond(LINUX_CAN_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
  check_include_files("${LINUX_CAN_HEADERS};linux/can.h" HAVE_LINUX_CAN_H)
  check_include_files("${LINUX_CAN_HEADERS};linux/can/bcm.h" HAVE_LINUX_CAN_BCM_H)
  check_include_files("${LINUX_CAN_HEADERS};linux/can/j1939.h" HAVE_LINUX_CAN_J1939_H)
  check_include_files("${LINUX_CAN_HEADERS};linux/can/raw.h" HAVE_LINUX_CAN_RAW_H)

  set(LINUX_VM_SOCKETS_HEADERS)
  add_cond(LINUX_VM_SOCKETS_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
  check_include_files("${LINUX_VM_SOCKETS_HEADERS};linux/vm_sockets.h" HAVE_LINUX_VM_SOCKETS_H)
endif()

check_include_files(mach-o/dyld.h HAVE_MACH_O_DYLD_H)
check_include_files(memory.h HAVE_MEMORY_H) # libffi and cpython
check_include_files(minix/config.h HAVE_MINIX_CONFIG_H)
check_include_files(ncurses.h HAVE_NCURSES_H)
check_include_files(ncurses/panel.h HAVE_NCURSES_PANEL_H)
check_include_files(netdb.h HAVE_NETDB_H)
check_include_files(netinet/in.h HAVE_NETINET_IN_H)
check_include_files(netpacket/packet.h HAVE_NETPACKET_PACKET_H)
check_include_files(panel.h HAVE_PANEL_H)
check_include_files(poll.h HAVE_POLL_H)
check_include_files(process.h HAVE_PROCESS_H)
check_include_files(pthread.h HAVE_PTHREAD_H)
check_include_files(pty.h HAVE_PTY_H)
check_include_files(pwd.h HAVE_PWD_H)
check_include_files("stdio.h;readline/readline.h" HAVE_READLINE_READLINE_H)
check_include_files(semaphore.h HAVE_SEMAPHORE_H)
check_include_files(shadow.h HAVE_SHADOW_H)
check_include_files(signal.h HAVE_SIGNAL_H)
check_include_files(spawn.h HAVE_SPAWN_H)
check_include_files(stdint.h HAVE_STDINT_H)   # libffi and cpython
check_include_files(stdlib.h HAVE_STDLIB_H)   # libffi and cpython
check_include_files(strings.h HAVE_STRINGS_H) # libffi and cpython
check_include_files(string.h HAVE_STRING_H)   # libffi and cpython
check_include_files(stropts.h HAVE_STROPTS_H)
check_include_files(sysexits.h HAVE_SYSEXITS_H)
check_include_files(sys/audioio.h HAVE_SYS_AUDIOIO_H)
check_include_files(sys/bsdtty.h HAVE_SYS_BSDTTY_H)
check_include_files(sys/epoll.h HAVE_SYS_EPOLL_H)
check_include_files(sys/event.h HAVE_SYS_EVENT_H)
check_include_files(sys/file.h HAVE_SYS_FILE_H)
check_include_files(sys/loadavg.h HAVE_SYS_LOADAVG_H)
check_include_files(sys/lock.h HAVE_SYS_LOCK_H)
check_include_files(sys/sysmacros.h HAVE_SYS_SYSMACROS_H)
check_include_files(sys/mkdev.h HAVE_SYS_MKDEV_H)
check_include_files(sys/mman.h HAVE_SYS_MMAN_H) # libffi and cpython
check_include_files(sys/modem.h HAVE_SYS_MODEM_H)
check_include_files(sys/param.h HAVE_SYS_PARAM_H)
check_include_files(sys/poll.h HAVE_SYS_POLL_H)
check_include_files(sys/random.h HAVE_SYS_RANDOM_H)
check_include_files(sys/resource.h HAVE_SYS_RESOURCE_H)
check_include_files(sys/select.h HAVE_SYS_SELECT_H)
check_include_files(sys/statvfs.h HAVE_SYS_STATVFS_H)
check_include_files(sys/stat.h HAVE_SYS_STAT_H) # libffi and cpython
check_include_files(sys/timeb.h HAVE_SYS_TIMEB_H)
check_include_files(sys/termio.h HAVE_SYS_TERMIO_H)
check_include_files(sys/times.h HAVE_SYS_TIMES_H)
check_include_files(sys/time.h HAVE_SYS_TIME_H)
check_include_files(sys/types.h HAVE_SYS_TYPES_H) # libffi and cpython
check_include_files(sys/un.h HAVE_SYS_UN_H)
check_include_files(sys/utsname.h HAVE_SYS_UTSNAME_H)
check_include_files(sys/wait.h HAVE_SYS_WAIT_H)
check_include_files(sys/auxv.h HAVE_SYS_AUXV_H)
check_include_files(termios.h HAVE_TERMIOS_H)
check_include_files(term.h HAVE_TERM_H)
if(IS_PY2)
  check_include_files(thread.h HAVE_THREAD_H)
endif()
check_include_files(unistd.h HAVE_UNISTD_H) # libffi and cpython
check_include_files(util.h HAVE_UTIL_H)
check_include_files(utime.h HAVE_UTIME_H)
check_include_files(wchar.h HAVE_WCHAR_H)
check_include_files("stdlib.h;stdarg.h;string.h;float.h" STDC_HEADERS) # libffi and cpython

check_include_files(stdarg.h HAVE_STDARG_PROTOTYPES)

if(IS_PY3)
  check_include_files(endian.h HAVE_ENDIAN_H)
  check_include_files(sched.h HAVE_SCHED_H)
  check_include_files(linux/memfd.h HAVE_LINUX_MEMFD_H)
  check_include_files(linux/random.h HAVE_LINUX_RANDOM_H)
  check_include_files(sys/devpoll.h HAVE_SYS_DEVPOLL_H)
  check_include_files(sys/endian.h HAVE_SYS_ENDIAN_H)
  check_include_files(sys/ioctl.h HAVE_SYS_IOCTL_H)
  check_include_files(sys/memfd.h HAVE_SYS_MEMFD_H)
  check_include_files("sys/types.h;sys/kern_control.h" HAVE_SYS_KERN_CONTROL_H)
  check_include_files(sys/sendfile.h HAVE_SYS_SENDFILE_H)
  check_include_files(sys/syscall.h HAVE_SYS_SYSCALL_H)
  check_include_files(sys/sys_domain.h HAVE_SYS_SYS_DOMAIN_H)
  check_include_files(sys/uio.h HAVE_SYS_UIO_H)
  check_include_files(sys/xattr.h HAVE_SYS_XATTR_H)

  # On Darwin (OS X) net/if.h requires sys/socket.h to be imported first.
  set(NET_IF_HEADERS stdio.h)
  if(STDC_HEADERS)
    set(NET_IF_HEADERS stdlib.h stddef.h)
  else()
    add_cond(NET_IF_HEADERS HAVE_STDLIB_H stdlib.h)
  endif()
  add_cond(NET_IF_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
  list(APPEND NET_IF_HEADERS net/if.h)
check_include_files("${NET_IF_HEADERS}" HAVE_NET_IF_H)

endif()

# FIXME: maybe need to remove, this is for posixmodule.c to open pty. But it seems that we don't need pty in python udf. It may influent some python library, but let's see if it works.
# find_file(HAVE_DEV_PTMX NAMES /dev/ptmx PATHS / NO_DEFAULT_PATH)
# find_file(HAVE_DEV_PTC  NAMES /dev/ptc  PATHS / NO_DEFAULT_PATH)
# message(STATUS "ptmx: ${HAVE_DEV_PTMX} ptc: ${HAVE_DEV_PTC}")

# FIXME: build crypt from source code
# find_library(HAVE_LIBCURSES curses)
# find_library(HAVE_LIBCRYPT crypt)
# if(NOT DEFINED HAVE_LIBDL)
#   set(HAVE_LIBDL ${CMAKE_DL_LIBS} CACHE STRING "Name of library containing dlopen and dlcose.")
# endif()
# find_library(HAVE_LIBDLD dld)
# find_library(HAVE_LIBINTL intl)

set(M_LIBRARIES )
check_function_exists("acosh" HAVE_BUILTIN_ACOSH)
if(HAVE_BUILTIN_ACOSH)
  # Math functions are builtin the environment (e.g emscripten)
  set(M_LIBRARIES )
else()
  message(FATAL "Could NOT find libm (missing: HAVE_LIBM)")
endif()

# FIXME: build ncurses from source code
# find_library(HAVE_LIBNCURSES ncurses)
# find_library(HAVE_LIBNSL nsl)
# find_library(HAVE_LIBREADLINE readline)
# if(IS_PY3)
# find_library(HAVE_LIBSENDFILE sendfile)
# endif()
# find_library(HAVE_LIBTERMCAP termcap)

set(LIBUTIL_LIBRARIES )
set(LIBUTIL_EXPECTED 1)

if(CMAKE_SYSTEM MATCHES "VxWorks\\-7$")
  set(LIBUTIL_EXPECTED 0)
  set(HAVE_LIBUTIL 0)
endif()

# FIXME: build libutil from source code
if(LIBUTIL_EXPECTED)
  check_function_exists("openpty" HAVE_BUILTIN_OPENPTY)
  if(HAVE_BUILTIN_OPENPTY)
    # Libutil functions are builtin the environment (e.g emscripten)
    set(LIBUTIL_LIBRARIES )
    set(HAVE_LIBUTIL 1)
  else()
    if(NOT DEFINED HAVE_LIBUTIL OR "${HAVE_LIBUTIL}" STREQUAL "")
      find_library(HAVE_LIBUTIL util)
      message(STATUS "Found libutil: ${HAVE_LIBUTIL}")
    endif()
    if(HAVE_LIBUTIL)
      set(LIBUTIL_LIBRARIES ${HAVE_LIBUTIL})
    endif()
  endif()
  if(NOT HAVE_LIBUTIL)
    message(FATAL_ERROR "Could NOT find libutil (missing: HAVE_LIBUTIL)")
  endif()
endif()

# FIXME: build CoreFoundation from source code
if(APPLE)
  find_library(HAVE_LIBCOREFOUNDATION CoreFoundation)
  find_library(HAVE_LIBSYSTEMCONFIGURATION SystemConfiguration)
endif()

if(WITH_THREAD OR PY_VERSION VERSION_GREATER_EQUAL "3.7")
  set(CMAKE_HAVE_PTHREAD_H ${HAVE_PTHREAD_H}) # Skip checking for header a second time.
  find_package(Threads)
  if(CMAKE_HAVE_LIBC_CREATE)
    set_required_def(_REENTRANT 1)
  endif()
endif()

# FIXME: check RAND_egd
# if(IS_PY3)
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_crypto_RAND_egd.c)
# file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
#   Use char because int might match the return type of a GCC
#   builtin and then its argument prototype would still apply.  */
# #ifdef __cplusplus
# extern \"C\"
# #endif
# char RAND_egd ();
# int main () { return RAND_egd (); }
# ")
# cmake_push_check_state()
# list(APPEND CMAKE_REQUIRED_LIBRARIES crypto)
# python_platform_test(
#   HAVE_RAND_EGD
#   "Checking for RAND_egd in -lcrypto"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()
# endif()

set_required_def(_POSIX_THREADS 1)    # Define on Linux as it is required for threading
set_required_def(_POSIX_SOURCE 1)     # Define on Linux in order for 'stat' and other things to work.
set_required_def(_GNU_SOURCE 1)       # Define on Linux to activate all library features
set_required_def(_NETBSD_SOURCE 1)    # Define on NetBSD to activate all library features
set_required_def(__BSD_VISIBLE 1)     # Define on FreeBSD to activate all library features
set_required_def(_BSD_TYPES 1)        # Define on Irix to enable u_int
set_required_def(_DARWIN_C_SOURCE 1)  # Define on Darwin to activate all library features

set_required_def(_ALL_SOURCE 1)       # Enable extensions on AIX 3, Interix.
set_required_def(_POSIX_PTHREAD_SEMANTICS 1) # Enable threading extensions on Solaris.
set_required_def(_TANDEM_SOURCE 1)    # Enable extensions on HP NonStop.

set_required_def(__EXTENSIONS__ 1)    # Defined on Solaris to see additional function prototypes.

if(HAVE_MINIX_CONFIG_H)
  set_required_def(_POSIX_1_SOURCE 2) # Define to 2 if the system does not provide POSIX.1 features except with this defined.
  set_required_def(_MINIX 1)          # Define to 1 if on MINIX.
endif()

message(STATUS "Checking for XOPEN_SOURCE")

# Some systems cannot stand _XOPEN_SOURCE being defined at all; they
# disable features if it is defined, without any means to access these
# features as extensions. For these systems, we skip the definition of
# _XOPEN_SOURCE. Before adding a system to the list to gain access to
# some feature, make sure there is no alternative way to access this
# feature. Also, when using wildcards, make sure you have verified the
# need for not defining _XOPEN_SOURCE on all systems matching the
# wildcard, and that the wildcard does not include future systems
# (which may remove their limitations).
set(define_xopen_source 1)

# On OpenBSD, select(2) is not available if _XOPEN_SOURCE is defined,
# even though select is a POSIX function. Reported by J. Ribbens.
# Reconfirmed for OpenBSD 3.3 by Zachary Hamm, for 3.4 by Jason Ish.
# In addition, Stefan Krah confirms that issue #1244610 exists through
# OpenBSD 4.6, but is fixed in 4.7.
if(CMAKE_SYSTEM MATCHES "OpenBSD\\-2\\."
   OR CMAKE_SYSTEM MATCHES "OpenBSD\\-3\\."
   OR CMAKE_SYSTEM MATCHES "OpenBSD\\-4\\.[0-6]$")

  #OpenBSD/2.* | OpenBSD/3.* | OpenBSD/4.@<:@0123456@:>@

  set(define_xopen_source 0)

  # OpenBSD undoes our definition of __BSD_VISIBLE if _XOPEN_SOURCE is
  # also defined. This can be overridden by defining _BSD_SOURCE
  # As this has a different meaning on Linux, only define it on OpenBSD
  set_required_def(_BSD_SOURCE 1)     # Define on OpenBSD to activate all library features

elseif(CMAKE_SYSTEM MATCHES OpenBSD)

  # OpenBSD/*

  # OpenBSD undoes our definition of __BSD_VISIBLE if _XOPEN_SOURCE is
  # also defined. This can be overridden by defining _BSD_SOURCE
  # As this has a different meaning on Linux, only define it on OpenBSD
  set_required_def(_BSD_SOURCE 1)     # Define on OpenBSD to activate all library features

elseif(CMAKE_SYSTEM MATCHES "NetBSD\\-1\\.5$"
       OR CMAKE_SYSTEM MATCHES "NetBSD\\-1\\.5\\."
       OR CMAKE_SYSTEM MATCHES "NetBSD\\-1\\.6$"
       OR CMAKE_SYSTEM MATCHES "NetBSD\\-1\\.6\\."
       OR CMAKE_SYSTEM MATCHES "NetBSD\\-1\\.6[A-S]$")

  # NetBSD/1.5 | NetBSD/1.5.* | NetBSD/1.6 | NetBSD/1.6.* | NetBSD/1.6@<:@A-S@:>@

  # Defining _XOPEN_SOURCE on NetBSD version prior to the introduction of
  # _NETBSD_SOURCE disables certain features (eg. setgroups). Reported by
  # Marc Recht
  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES SunOS)

  # SunOS/*)

  # From the perspective of Solaris, _XOPEN_SOURCE is not so much a
  # request to enable features supported by the standard as a request
  # to disable features not supported by the standard.  The best way
  # for Python to use Solaris is simply to leave _XOPEN_SOURCE out
  # entirely and define __EXTENSIONS__ instead.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "OpenUNIX\\-8\\.0\\.0$"
       OR CMAKE_SYSTEM MATCHES "UnixWare\\-7\\.1\\.[0-4]$")

  # OpenUNIX/8.0.0| UnixWare/7.1.@<:@0-4@:>@

  # On UnixWare 7, u_long is never defined with _XOPEN_SOURCE,
  # but used in /usr/include/netinet/tcp.h. Reported by Tim Rice.
  # Reconfirmed for 7.1.4 by Martin v. Loewis.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "SCO_SV\\-3\\.2$")

  # SCO_SV/3.2

  # On OpenServer 5, u_short is never defined with _XOPEN_SOURCE,
  # but used in struct sockaddr.sa_family. Reported by Tim Rice.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "FreeBSD\\-4\\.")

  # FreeBSD/4.*

  # On FreeBSD 4, the math functions C89 does not cover are never defined
  # with _XOPEN_SOURCE and __BSD_VISIBLE does not re-enable them.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "Darwin\\-[6789]\\."
       OR CMAKE_SYSTEM MATCHES "Darwin\\-1[0-9]\\.")

  # Darwin/@<:@6789@:>@.*)
  # Darwin/1@<:@0-9@:>@.*

  # On MacOS X 10.2, a bug in ncurses.h means that it craps out if
  # _XOPEN_EXTENDED_SOURCE is defined. Apparently, this is fixed in 10.3, which
  # identifies itself as Darwin/7.*
  # On Mac OS X 10.4, defining _POSIX_C_SOURCE or _XOPEN_SOURCE
  # disables platform specific features beyond repair.
  # On Mac OS X 10.3, defining _POSIX_C_SOURCE or _XOPEN_SOURCE
  # has no effect, don't bother defining them

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "AIX\\-4$"
       OR CMAKE_SYSTEM MATCHES "AIX\\-5\\.1$")
  # On AIX 4 and 5.1, mbstate_t is defined only when _XOPEN_SOURCE == 500 but
  # used in wcsnrtombs() and mbsnrtowcs() even if _XOPEN_SOURCE is not defined
  # or has another value. By not (re)defining it, the defaults come in place.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "QNX\\-6\\.3\\.2$")

  # QNX/6.3.2

  # On QNX 6.3.2, defining _XOPEN_SOURCE prevents netdb.h from
  # defining NI_NUMERICHOST.

  set(define_xopen_source 0)

elseif(CMAKE_SYSTEM MATCHES "VxWorks\\-7$")

  # VxWorks-7

  # On VxWorks-7, defining _XOPEN_SOURCE or _POSIX_C_SOURCE
  # leads to a failure in select.h because sys/types.h fails
  # to define FD_SETSIZE.
  # Reported by Martin Oberhuber as V7COR-4651.

  set(define_xopen_source 0)

endif()

# FIXME: check xopen_source
# if(define_xopen_source)
#   message(STATUS "Checking for XOPEN_SOURCE - yes")
#   set_required_def(_XOPEN_SOURCE_EXTENDED 1) # Define to activate Unix95-and-earlier features
#   if(IS_PY2)
#     set_required_def(_XOPEN_SOURCE 600)        # Define to the level of X/Open that your system supports
#     set_required_def(_POSIX_C_SOURCE 200112L)  # Define to activate features from IEEE Stds 1003.1-2001
#   else()
#     set_required_def(_XOPEN_SOURCE 700)        # Define to the level of X/Open that your system supports
#     set_required_def(_POSIX_C_SOURCE 200809L)  # Define to activate features from IEEE Stds 1003.1-2008
#   endif()
# else()
#   message(STATUS "Checking for XOPEN_SOURCE - no")
# endif()

# FIXME: check large file support
# message(STATUS "Checking for Large File Support")
# set(use_lfs 1)  # Consider disabling "lfs" if porting to Solaris (2.6 to 9) with gcc 2.95.
#                 # See associated test in configure.in
# if(use_lfs)
#   message(STATUS "Checking for Large File Support - yes")
#   if(CMAKE_SYSTEM MATCHES AIX)
#     set_required_def(_LARGE_FILES 1)        # This must be defined on AIX systems to enable large file support.
#   endif()
#   set_required_def(_LARGEFILE_SOURCE 1)     # This must be defined on some systems to enable large file support.
#   set_required_def(_FILE_OFFSET_BITS 64)    # This must be set to 64 on some systems to enable large file support.
# else()
#   message(STATUS "Checking for Large File Support - no")
# endif()

# FIXME: CMAKE_REQUIRED_LIBRARIES will effect the global environment, using target_link_libraries instead
# # CMAKE_EXTRA_INCLUDE_FILES is used in CheckTypeSize
# set(CMAKE_EXTRA_INCLUDE_FILES stdio.h)

# add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBM "${M_LIBRARIES}")
# add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBINTL ${HAVE_LIBINTL})
# add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBUTIL "${LIBUTIL_LIBRARIES}")
# add_cond(CMAKE_EXTRA_INCLUDE_FILES HAVE_WCHAR_H wchar.h)

# TEST_BIG_ENDIAN(WORDS_BIGENDIAN)

check_type_size(double SIZEOF_DOUBLE) # libffi and cpython
check_type_size(float SIZEOF_FLOAT)
check_type_size(fpos_t SIZEOF_FPOS_T)
check_type_size(int SIZEOF_INT)
check_type_size(long SIZEOF_LONG)
check_type_size("long double" SIZEOF_LONG_DOUBLE)
set(HAVE_LONG_DOUBLE ${SIZEOF_LONG_DOUBLE}) # libffi and cpython
check_type_size("long long" SIZEOF_LONG_LONG)
set(HAVE_LONG_LONG ${SIZEOF_LONG_LONG})
check_type_size(off_t SIZEOF_OFF_T)
check_type_size(pid_t SIZEOF_PID_T)
check_type_size(pthread_t SIZEOF_PTHREAD_T)
check_type_size(short SIZEOF_SHORT)
check_type_size(size_t SIZEOF_SIZE_T)
check_type_size(ssize_t HAVE_SSIZE_T)
check_type_size(time_t SIZEOF_TIME_T)
check_type_size(uintptr_t SIZEOF_UINTPTR_T)
set(HAVE_UINTPTR_T ${SIZEOF_UINTPTR_T})
check_type_size("void *" SIZEOF_VOID_P)
check_type_size(wchar_t SIZEOF_WCHAR_T)
check_type_size(_Bool SIZEOF__BOOL)
set(HAVE_C99_BOOL ${SIZEOF__BOOL})

# FIXME: fix compile error, link gcc_s?
# Check the alignment of size_t
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/alignof_size_t.c)
# file(WRITE ${check_src} "#include <stddef.h>
# int main() {
#     struct { char c; size_t x; } s;
#     return offsetof(struct { char c; size_t x; }, x);
# }
# ")
# python_platform_test_run_get_exit_code(
#   ALIGNOF_SIZE_T
#   "Check alignment of size_t"
#   ${check_src}
#   DIRECT
#   )

SET(ALIGNOF_LONG 8)
SET(ALIGNOF_SIZE_T 8)

# libffi specific: Check whether more than one size of the long double type is supported
set(HAVE_LONG_DOUBLE_VARIANT 0)

# TODO Improve AIX support setting AIX_GENUINE_CPLUSPLUS and AIX_BUILDDATE
#      See https://github.com/python-cmake-buildsystem/python-cmake-buildsystem/issues/296
set(AIX_GENUINE_CPLUSPLUS 0)

set(WITH_DYLD 0)
set(WITH_NEXT_FRAMEWORK 0)
if(APPLE)
  set(WITH_DYLD 1)
  set(WITH_NEXT_FRAMEWORK 0) # TODO: See --enable-framework option.
endif()
set(PYTHONFRAMEWORK "")

if(HAVE_LONG_LONG)
  if(SIZEOF_OFF_T GREATER SIZEOF_LONG
      AND (SIZEOF_LONG_LONG GREATER SIZEOF_OFF_T OR SIZEOF_LONG_LONG EQUAL SIZEOF_OFF_T))
      set(HAVE_LARGEFILE_SUPPORT 1)
  endif()

endif()

set(CFG_HEADERS )

add_cond(CFG_HEADERS HAVE_SYS_EPOLL_H sys/epoll.h)
add_cond(CFG_HEADERS HAVE_SYS_EVENT_H sys/event.h)
add_cond(CFG_HEADERS HAVE_SYS_RANDOM_H sys/random.h)
add_cond(CFG_HEADERS HAVE_SYS_SYSMACROS_H sys/sysmacros.h)
add_cond(CFG_HEADERS HAVE_SYS_TYPES_H sys/types.h)
add_cond(CFG_HEADERS HAVE_SYS_TIME_H sys/time.h)
add_cond(CFG_HEADERS HAVE_SYS_FILE_H sys/file.h)
add_cond(CFG_HEADERS HAVE_SYS_POLL_H sys/poll.h)
add_cond(CFG_HEADERS HAVE_SYS_STATVFS_H sys/statvfs.h)
add_cond(CFG_HEADERS HAVE_SYS_STAT_H sys/stat.h)
add_cond(CFG_HEADERS HAVE_SYS_LOCK_H sys/lock.h)
add_cond(CFG_HEADERS HAVE_SYS_TIMEB_H sys/timeb.h)
add_cond(CFG_HEADERS HAVE_SYS_TIMES_H sys/times.h)
add_cond(CFG_HEADERS HAVE_SYS_UIO_H sys/uio.h)
add_cond(CFG_HEADERS HAVE_SYS_UTSNAME_H sys/utsname.h)
add_cond(CFG_HEADERS HAVE_SYS_MMAN_H sys/mman.h)
add_cond(CFG_HEADERS HAVE_SYS_SOCKET_H sys/socket.h)
add_cond(CFG_HEADERS HAVE_SYS_WAIT_H sys/wait.h)
add_cond(CFG_HEADERS HAVE_PWD_H pwd.h)
add_cond(CFG_HEADERS HAVE_GRP_H grp.h)
add_cond(CFG_HEADERS HAVE_SHADOW_H shadow.h)
add_cond(CFG_HEADERS HAVE_INTTYPES_H inttypes.h)
add_cond(CFG_HEADERS HAVE_LOCALE_H locale.h)
add_cond(CFG_HEADERS HAVE_LIBINTL_H libintl.h)
add_cond(CFG_HEADERS HAVE_FCNTL_H fcntl.h)
add_cond(CFG_HEADERS HAVE_PTY_H pty.h)
add_cond(CFG_HEADERS HAVE_SIGNAL_H signal.h)
add_cond(CFG_HEADERS HAVE_STDINT_H stdint.h)
add_cond(CFG_HEADERS HAVE_STDLIB_H stdlib.h)
add_cond(CFG_HEADERS HAVE_STRING_H string.h)
add_cond(CFG_HEADERS HAVE_UTIL_H util.h)
add_cond(CFG_HEADERS HAVE_UNISTD_H unistd.h)
add_cond(CFG_HEADERS HAVE_UTIME_H utime.h)
add_cond(CFG_HEADERS HAVE_WCHAR_H wchar.h)
if(IS_PY3)
add_cond(CFG_HEADERS HAVE_DIRENT_H dirent.h)
add_cond(CFG_HEADERS HAVE_ENDIAN_H endian.h)
add_cond(CFG_HEADERS HAVE_LINUX_MEMFD_H linux/memfd.h)
add_cond(CFG_HEADERS HAVE_LINUX_WAIT_H linux/wait.h)
add_cond(CFG_HEADERS HAVE_MACH_O_DYLD_H mach-o/dyld.h)
add_cond(CFG_HEADERS HAVE_NET_IF_H net/if.h)
add_cond(CFG_HEADERS HAVE_SCHED_H sched.h)
add_cond(CFG_HEADERS HAVE_SYS_ENDIAN_H sys/endian.h)
add_cond(CFG_HEADERS HAVE_SYS_MEMFD_H sys/memfd.h)
add_cond(CFG_HEADERS HAVE_SYS_RESOURCE_H sys/resource.h)
add_cond(CFG_HEADERS HAVE_SYS_SENDFILE_H sys/sendfile.h)
add_cond(CFG_HEADERS HAVE_SYS_TIME_H sys/time.h)
endif()

if(HAVE_PTY_H)
  set(CFG_HEADERS ${CFG_HEADERS} pty.h utmp.h)
endif()

set(CFG_HEADERS ${CFG_HEADERS} time.h stdio.h math.h)

check_symbol_exists(alarm        "${CFG_HEADERS}" HAVE_ALARM)
check_symbol_exists(alloca       "${CFG_HEADERS}" HAVE_ALLOCA) # libffi and cpython
check_symbol_exists(altzone      "${CFG_HEADERS}" HAVE_ALTZONE)
check_symbol_exists(bind_textdomain_codeset "${CFG_HEADERS}" HAVE_BIND_TEXTDOMAIN_CODESET)
check_symbol_exists(chflags      "${CFG_HEADERS}" HAVE_CHFLAGS)
check_symbol_exists(chown        "${CFG_HEADERS}" HAVE_CHOWN)
check_symbol_exists(chroot       "${CFG_HEADERS}" HAVE_CHROOT)
check_symbol_exists(clock        "${CFG_HEADERS}" HAVE_CLOCK)
check_symbol_exists(confstr      "${CFG_HEADERS}" HAVE_CONFSTR)
check_symbol_exists(ctermid      "${CFG_HEADERS}" HAVE_CTERMID)
check_symbol_exists(ctermid_r    "${CFG_HEADERS}" HAVE_CTERMID_R)
check_symbol_exists(dup2         "${CFG_HEADERS}" HAVE_DUP2)
check_symbol_exists(epoll_create "${CFG_HEADERS}" HAVE_EPOLL)
if(IS_PY3)
  check_symbol_exists(epoll_create1 "${CFG_HEADERS}" HAVE_EPOLL_CREATE1)
endif()
check_symbol_exists(execv        "${CFG_HEADERS}" HAVE_EXECV)
check_symbol_exists(fchdir       "${CFG_HEADERS}" HAVE_FCHDIR)
check_symbol_exists(fchmod       "${CFG_HEADERS}" HAVE_FCHMOD)
check_symbol_exists(fchown       "${CFG_HEADERS}" HAVE_FCHOWN)
check_symbol_exists(fdatasync    "${CFG_HEADERS}" HAVE_FDATASYNC)
check_symbol_exists(flock        "${CFG_HEADERS}" HAVE_FLOCK)
if(NOT HAVE_FLOCK)
  check_library_exists(bsd flock "" FLOCK_NEEDS_LIBBSD)
endif()
check_symbol_exists(fork         "${CFG_HEADERS}" HAVE_FORK)
check_symbol_exists(forkpty      "${CFG_HEADERS}" HAVE_FORKPTY)
check_symbol_exists(fpathconf    "${CFG_HEADERS}" HAVE_FPATHCONF)
cmake_push_check_state()
set(CFG_HEADERS_SAVE ${CFG_HEADERS})
add_cond(CFG_HEADERS HAVE_FPU_CONTROL_H fpu_control.h)
check_symbol_exists(__fpu_control  "${CFG_HEADERS}" HAVE___FPU_CONTROL)
if(NOT HAVE___FPU_CONTROL)
  check_library_exists(ieee __fpu_control "" HAVE_LIBIEEE)
endif()
set(CFG_HEADERS ${CFG_HEADERS_SAVE})
cmake_pop_check_state()
check_symbol_exists(fseek64      "${CFG_HEADERS}" HAVE_FSEEK64)
check_symbol_exists(fseeko       "${CFG_HEADERS}" HAVE_FSEEKO)
check_symbol_exists(fstatvfs     "${CFG_HEADERS}" HAVE_FSTATVFS)
check_symbol_exists(fsync        "${CFG_HEADERS}" HAVE_FSYNC)
check_symbol_exists(ftell64      "${CFG_HEADERS}" HAVE_FTELL64)
check_symbol_exists(ftello       "${CFG_HEADERS}" HAVE_FTELLO)
check_symbol_exists(ftime        "${CFG_HEADERS}" HAVE_FTIME)
check_symbol_exists(ftruncate    "${CFG_HEADERS}" HAVE_FTRUNCATE)
if(IS_PY2)
  check_symbol_exists(getcwd       "${CFG_HEADERS}" HAVE_GETCWD)
endif()
check_symbol_exists(getc_unlocked   "${CFG_HEADERS}" HAVE_GETC_UNLOCKED)
check_symbol_exists(getgroups       "${CFG_HEADERS}" HAVE_GETGROUPS)
check_symbol_exists(getitimer    "${CFG_HEADERS}" HAVE_GETITIMER)
check_symbol_exists(getloadavg   "${CFG_HEADERS}" HAVE_GETLOADAVG)
check_symbol_exists(getlogin     "${CFG_HEADERS}" HAVE_GETLOGIN)
check_symbol_exists(getpagesize  "${CFG_HEADERS}" HAVE_GETPAGESIZE)
check_symbol_exists(getpgid      "${CFG_HEADERS}" HAVE_GETPGID)
check_symbol_exists(getpgrp      "${CFG_HEADERS}" HAVE_GETPGRP)
check_symbol_exists(getpid       "${CFG_HEADERS}" HAVE_GETPID)
python_check_function(getpriority HAVE_GETPRIORITY)
check_symbol_exists(getpwent     "${CFG_HEADERS}" HAVE_GETPWENT)
check_symbol_exists(getresgid    "${CFG_HEADERS}" HAVE_GETRESGID)
check_symbol_exists(getresuid    "${CFG_HEADERS}" HAVE_GETRESUID)
check_symbol_exists(getsid       "${CFG_HEADERS}" HAVE_GETSID)
check_symbol_exists(getspent     "${CFG_HEADERS}" HAVE_GETSPENT)
check_symbol_exists(getspnam     "${CFG_HEADERS}" HAVE_GETSPNAM)
check_symbol_exists(gettimeofday "${CFG_HEADERS}" HAVE_GETTIMEOFDAY)
check_symbol_exists(getwd        "${CFG_HEADERS}" HAVE_GETWD)
check_symbol_exists(hypot        "${CFG_HEADERS}" HAVE_HYPOT)
check_symbol_exists(initgroups   "${CFG_HEADERS}" HAVE_INITGROUPS)
check_symbol_exists(kill         "${CFG_HEADERS}" HAVE_KILL)
check_symbol_exists(killpg       "${CFG_HEADERS}" HAVE_KILLPG)
check_symbol_exists(kqueue       "${CFG_HEADERS}" HAVE_KQUEUE)
check_symbol_exists(lchflags     "${CFG_HEADERS}" HAVE_LCHFLAGS)
python_check_function(lchmod HAVE_LCHMOD)
check_symbol_exists(lchown       "${CFG_HEADERS}" HAVE_LCHOWN)
check_symbol_exists(link         "${CFG_HEADERS}" HAVE_LINK)
check_symbol_exists(lstat        "${CFG_HEADERS}" HAVE_LSTAT)
check_symbol_exists(makedev      "${CFG_HEADERS}" HAVE_MAKEDEV)
check_symbol_exists(memcpy       "${CFG_HEADERS}" HAVE_MEMCPY) # libffi and cpython
check_symbol_exists(memmove      "${CFG_HEADERS}" HAVE_MEMMOVE)
check_symbol_exists(mkfifo       "${CFG_HEADERS}" HAVE_MKFIFO)
check_symbol_exists(mknod        "${CFG_HEADERS}" HAVE_MKNOD)
check_symbol_exists(mktime       "${CFG_HEADERS}" HAVE_MKTIME)
check_symbol_exists(mmap         "${CFG_HEADERS}" HAVE_MMAP) # libffi and cpython
check_symbol_exists(mremap       "${CFG_HEADERS}" HAVE_MREMAP)
check_symbol_exists(nice         "${CFG_HEADERS}" HAVE_NICE)
check_symbol_exists(openpty      "${CFG_HEADERS}" HAVE_OPENPTY)
check_symbol_exists(pathconf     "${CFG_HEADERS}" HAVE_PATHCONF)
check_symbol_exists(pause        "${CFG_HEADERS}" HAVE_PAUSE)
check_symbol_exists(plock        "${CFG_HEADERS}" HAVE_PLOCK)
check_symbol_exists(poll         "${CFG_HEADERS}" HAVE_POLL)
check_symbol_exists(putenv       "${CFG_HEADERS}" HAVE_PUTENV)
check_symbol_exists(readlink     "${CFG_HEADERS}" HAVE_READLINK)
check_symbol_exists(realpath     "${CFG_HEADERS}" HAVE_REALPATH)
check_symbol_exists(select       "${CFG_HEADERS}" HAVE_SELECT)
check_symbol_exists(setegid      "${CFG_HEADERS}" HAVE_SETEGID)
check_symbol_exists(seteuid      "${CFG_HEADERS}" HAVE_SETEUID)
check_symbol_exists(setgid       "${CFG_HEADERS}" HAVE_SETGID)
check_symbol_exists(setgroups    "${CFG_HEADERS}" HAVE_SETGROUPS)
check_symbol_exists(setitimer    "${CFG_HEADERS}" HAVE_SETITIMER)
check_symbol_exists(setlocale    "${CFG_HEADERS}" HAVE_SETLOCALE)
check_symbol_exists(setpgid      "${CFG_HEADERS}" HAVE_SETPGID)
check_symbol_exists(setpgrp      "${CFG_HEADERS}" HAVE_SETPGRP)
check_symbol_exists(setregid     "${CFG_HEADERS}" HAVE_SETREGID)
check_symbol_exists(setreuid     "${CFG_HEADERS}" HAVE_SETREUID)
check_symbol_exists(setresgid    "${CFG_HEADERS}" HAVE_SETRESGID)
check_symbol_exists(setresuid    "${CFG_HEADERS}" HAVE_SETRESUID)
check_symbol_exists(setsid       "${CFG_HEADERS}" HAVE_SETSID)
check_symbol_exists(setuid       "${CFG_HEADERS}" HAVE_SETUID)
check_symbol_exists(setvbuf      "${CFG_HEADERS}" HAVE_SETVBUF)
check_symbol_exists(sigaction    "${CFG_HEADERS}" HAVE_SIGACTION)
check_symbol_exists(siginterrupt "${CFG_HEADERS}" HAVE_SIGINTERRUPT)
check_symbol_exists(sigrelse     "${CFG_HEADERS}" HAVE_SIGRELSE)
check_symbol_exists(snprintf     "${CFG_HEADERS}" HAVE_SNPRINTF)
check_symbol_exists(socketpair   "${CFG_HEADERS}" HAVE_SOCKETPAIR)
check_symbol_exists(statvfs      "${CFG_HEADERS}" HAVE_STATVFS)
check_symbol_exists(strdup       "${CFG_HEADERS}" HAVE_STRDUP)
check_symbol_exists(strftime     "${CFG_HEADERS}" HAVE_STRFTIME)
check_symbol_exists(symlink      "${CFG_HEADERS}" HAVE_SYMLINK)
check_symbol_exists(sysconf      "${CFG_HEADERS}" HAVE_SYSCONF)
check_symbol_exists(tcgetpgrp    "${CFG_HEADERS}" HAVE_TCGETPGRP)
check_symbol_exists(tcsetpgrp    "${CFG_HEADERS}" HAVE_TCSETPGRP)
check_symbol_exists(tempnam      "${CFG_HEADERS}" HAVE_TEMPNAM)
check_symbol_exists(timegm       "${CFG_HEADERS}" HAVE_TIMEGM)
check_symbol_exists(times        "${CFG_HEADERS}" HAVE_TIMES)
check_symbol_exists(tmpfile      "${CFG_HEADERS}" HAVE_TMPFILE)
check_symbol_exists(tmpnam       "${CFG_HEADERS}" HAVE_TMPNAM)
check_symbol_exists(tmpnam_r     "${CFG_HEADERS}" HAVE_TMPNAM_R)
check_symbol_exists(truncate     "${CFG_HEADERS}" HAVE_TRUNCATE)
check_symbol_exists(uname        "${CFG_HEADERS}" HAVE_UNAME)
check_symbol_exists(unsetenv     "${CFG_HEADERS}" HAVE_UNSETENV)
check_symbol_exists(utimes       "${CFG_HEADERS}" HAVE_UTIMES)
check_symbol_exists(wait3        "${CFG_HEADERS}" HAVE_WAIT3)
check_symbol_exists(wait4        "${CFG_HEADERS}" HAVE_WAIT4)
check_symbol_exists(waitpid      "${CFG_HEADERS}" HAVE_WAITPID)
check_symbol_exists(wcscoll      "${CFG_HEADERS}" HAVE_WCSCOLL)
check_symbol_exists(_getpty      "${CFG_HEADERS}" HAVE__GETPTY)

if(IS_PY3)
  check_symbol_exists(accept4      "${CFG_HEADERS}" HAVE_ACCEPT4)
  check_symbol_exists(copy_file_range "${CFG_HEADERS}" HAVE_COPY_FILE_RANGE)
  check_symbol_exists(dup3         "${CFG_HEADERS}" HAVE_DUP3)
  check_symbol_exists(_dyld_shared_cache_contains_path "${CFG_HEADERS}" HAVE_DYLD_SHARED_CACHE_CONTAINS_PATH)
  check_symbol_exists(explicit_bzero  "${CFG_HEADERS}" HAVE_EXPLICIT_BZERO)
  check_symbol_exists(explicit_memset "${CFG_HEADERS}" HAVE_EXPLICIT_MEMSET)
  check_symbol_exists(faccessat    "${CFG_HEADERS}" HAVE_FACCESSAT)
  check_symbol_exists(fchmodat     "${CFG_HEADERS}" HAVE_FCHMODAT)
  check_symbol_exists(fchownat     "${CFG_HEADERS}" HAVE_FCHOWNAT)
  check_symbol_exists(fdwalk       "${CFG_HEADERS}" HAVE_FDWALK)
  check_symbol_exists(fexecve      "${CFG_HEADERS}" HAVE_FEXECVE)
  check_symbol_exists(fdopendir    "${CFG_HEADERS}" HAVE_FDOPENDIR)
  check_symbol_exists(fstatat      "${CFG_HEADERS}" HAVE_FSTATAT)
  check_symbol_exists(futimens     "${CFG_HEADERS}" HAVE_FUTIMENS)
  check_symbol_exists(futimes      "${CFG_HEADERS}" HAVE_FUTIMES)
  check_symbol_exists(futimesat    "${CFG_HEADERS}" HAVE_FUTIMESAT)
  check_symbol_exists(getentropy   "${CFG_HEADERS}" HAVE_GETENTROPY)
  python_check_function(getpriority HAVE_GETPRIORITY)
  check_symbol_exists(getgrgid_r   "${CFG_HEADERS}" HAVE_GETGRGID_R)
  check_symbol_exists(getgrnam_r   "${CFG_HEADERS}" HAVE_GETGRNAM_R)
  check_symbol_exists(getgrouplist "${CFG_HEADERS}" HAVE_GETGROUPLIST)
  check_symbol_exists(getpwnam_r   "${CFG_HEADERS}" HAVE_GETPWNAM_R)
  check_symbol_exists(getpwuid_r   "${CFG_HEADERS}" HAVE_GETPWUID_R)
  check_symbol_exists(htole64      "${CFG_HEADERS}" HAVE_HTOLE64)
  check_symbol_exists(if_nameindex "${CFG_HEADERS}" HAVE_IF_NAMEINDEX)
  check_symbol_exists(linkat       "${CFG_HEADERS}" HAVE_LINKAT)
  check_symbol_exists(lockf        "${CFG_HEADERS}" HAVE_LOCKF)
  check_symbol_exists(lutimes      "${CFG_HEADERS}" HAVE_LUTIMES)
  check_symbol_exists(madvise      "${CFG_HEADERS}" HAVE_MADVISE)
  check_symbol_exists(mbrtowc      "${CFG_HEADERS}" HAVE_MBRTOWC)
  check_symbol_exists(memfd_create "${CFG_HEADERS}" HAVE_MEMFD_CREATE)
  check_symbol_exists(memrchr      "${CFG_HEADERS}" HAVE_MEMRCHR)
  check_symbol_exists(mkdirat      "${CFG_HEADERS}" HAVE_MKDIRAT)
  check_symbol_exists(mkfifoat     "${CFG_HEADERS}" HAVE_MKFIFOAT)
  check_symbol_exists(mknodat      "${CFG_HEADERS}" HAVE_MKNODAT)
  check_symbol_exists(openat       "${CFG_HEADERS}" HAVE_OPENAT)
  check_symbol_exists(pipe2        "${CFG_HEADERS}" HAVE_PIPE2)
  check_symbol_exists(posix_fadvise          "${CFG_HEADERS}" HAVE_POSIX_FADVISE)
  check_symbol_exists(posix_fallocate        "${CFG_HEADERS}" HAVE_POSIX_FALLOCATE)
  check_symbol_exists(posix_spawn            "${CFG_HEADERS}" HAVE_POSIX_SPAWN)
  check_symbol_exists(posix_spawnp           "${CFG_HEADERS}" HAVE_POSIX_SPAWNP)
  check_symbol_exists(pread                  "${CFG_HEADERS}" HAVE_PREAD)
  check_symbol_exists(preadv                 "${CFG_HEADERS}" HAVE_PREADV)
  check_symbol_exists(preadv2                "${CFG_HEADERS}" HAVE_PREADV2)
  check_symbol_exists(prlimit                "${CFG_HEADERS}" HAVE_PRLIMIT)

  cmake_push_check_state()
  list(APPEND CMAKE_REQUIRED_LIBRARIES pthread)
  check_symbol_exists(pthread_kill           "${CFG_HEADERS}" HAVE_PTHREAD_KILL)
  cmake_pop_check_state()

  check_symbol_exists(pwrite                 "${CFG_HEADERS}" HAVE_PWRITE)
  check_symbol_exists(pwritev                "${CFG_HEADERS}" HAVE_PWRITEV)
  check_symbol_exists(pwritev2               "${CFG_HEADERS}" HAVE_PWRITEV2)
  check_symbol_exists(readlinkat             "${CFG_HEADERS}" HAVE_READLINKAT)
  check_symbol_exists(readv                  "${CFG_HEADERS}" HAVE_READV)
  check_symbol_exists(renameat               "${CFG_HEADERS}" HAVE_RENAMEAT)
  check_symbol_exists(rtpSpawn               "${CFG_HEADERS}" HAVE_RTPSPAWN)
  check_symbol_exists(sched_rr_get_interval  "${CFG_HEADERS}" HAVE_SCHED_RR_GET_INTERVAL)
  check_symbol_exists(sched_setaffinity      "${CFG_HEADERS}" HAVE_SCHED_SETAFFINITY)
  check_symbol_exists(sched_setparam         "${CFG_HEADERS}" HAVE_SCHED_SETPARAM)
  check_symbol_exists(sched_setscheduler     "${CFG_HEADERS}" HAVE_SCHED_SETSCHEDULER)
  check_symbol_exists(sendfile               "${CFG_HEADERS}" HAVE_SENDFILE)
  check_symbol_exists(sethostname            "${CFG_HEADERS}" HAVE_SETHOSTNAME)
  check_symbol_exists(setpriority            "${CFG_HEADERS}" HAVE_SETPRIORITY)
  check_symbol_exists(sched_get_priority_max "${CFG_HEADERS}" HAVE_SCHED_GET_PRIORITY_MAX)
  check_symbol_exists(sigaltstack            "${CFG_HEADERS}" HAVE_SIGALTSTACK)
  check_symbol_exists(sigfillset             "${CFG_HEADERS}" HAVE_SIGFILLSET)
  check_symbol_exists(sigpending             "${CFG_HEADERS}" HAVE_SIGPENDING)
  check_symbol_exists(strsignal              "${CFG_HEADERS}" HAVE_STRSIGNAL)
  check_symbol_exists(sigtimedwait           "${CFG_HEADERS}" HAVE_SIGTIMEDWAIT)
  check_symbol_exists(sigwait                "${CFG_HEADERS}" HAVE_SIGWAIT)
  check_symbol_exists(sigwaitinfo            "${CFG_HEADERS}" HAVE_SIGWAITINFO)
  check_symbol_exists(strlcpy                "${CFG_HEADERS}" HAVE_STRLCPY)
  check_symbol_exists(symlinkat              "${CFG_HEADERS}" HAVE_SYMLINKAT)
  check_symbol_exists(sync                   "${CFG_HEADERS}" HAVE_SYNC)
  check_symbol_exists(unlinkat               "${CFG_HEADERS}" HAVE_UNLINKAT)
  check_symbol_exists(utimensat              "${CFG_HEADERS}" HAVE_UTIMENSAT)
  check_symbol_exists(waitid                 "${CFG_HEADERS}" HAVE_WAITID)
  check_symbol_exists(wcsftime               "${CFG_HEADERS}" HAVE_WCSFTIME)
  check_symbol_exists(wcsxfrm                "${CFG_HEADERS}" HAVE_WCSXFRM)
  check_symbol_exists(wmemcmp                "${CFG_HEADERS}" HAVE_WMEMCMP)
  check_symbol_exists(writev                 "${CFG_HEADERS}" HAVE_WRITEV)
endif()

check_struct_has_member("struct stat" st_mtim.tv_nsec "${CFG_HEADERS}" HAVE_STAT_TV_NSEC)
check_struct_has_member("struct stat" st_mtimespec.tv_nsec "${CFG_HEADERS}"    HAVE_STAT_TV_NSEC2)
check_struct_has_member("struct stat" st_birthtime "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_BIRTHTIME)
check_struct_has_member("struct stat" st_blksize "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_BLKSIZE)
check_struct_has_member("struct stat" st_blocks  "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_BLOCKS)
set(HAVE_ST_BLOCKS ${HAVE_STRUCT_STAT_ST_BLOCKS})
check_struct_has_member("struct stat" st_flags   "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_FLAGS)
check_struct_has_member("struct stat" st_gen     "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_GEN)
check_struct_has_member("struct stat" st_rdev    "${CFG_HEADERS}"    HAVE_STRUCT_STAT_ST_RDEV)

if(IS_PY3)
  check_struct_has_member("struct passwd" pw_gecos  "${CFG_HEADERS}" HAVE_STRUCT_PASSWD_PW_GECOS)
  check_struct_has_member("struct passwd" pw_passwd "${CFG_HEADERS}" HAVE_STRUCT_PASSWD_PW_PASSWD)

  check_struct_has_member("struct siginfo_t" si_band "${CFG_HEADERS}" HAVE_SIGINFO_T_SI_BAND)
endif()

#######################################################################
#
# Check for shm
#
#######################################################################
if(PY_VERSION VERSION_GREATER_EQUAL "3.8")

foreach(func IN ITEMS shm_open shm_unlink)
  string(TOUPPER ${func} _func_upper)
  check_symbol_exists(${func} "${CFG_HEADERS}" HAVE_${_func_upper})
  if(NOT HAVE_${_func_upper})
    cmake_push_check_state()
    list(APPEND CMAKE_REQUIRED_LIBRARIES rt)
    check_symbol_exists(${func} "${CFG_HEADERS}" HAVE_${_func_upper}_IN_RT)
    cmake_pop_check_state()
    set(SHM_NEEDS_LIBRT ${HAVE_${_func_upper}_IN_RT})
    set(HAVE_${_func_upper} ${HAVE_${_func_upper}_IN_RT})
  endif()
endforeach()

endif()

#######################################################################
#
# Check for gcc x64 inline assembler
#
#######################################################################
if(IS_PY3)

# Check for x64 gcc inline assembler
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gcc_asm_for_x64.c)
file(WRITE ${check_src} "int main () {
  __asm__ __volatile__ (\"movq %rcx, %rax\");
}
")
python_platform_test(
  HAVE_GCC_ASM_FOR_X64
  "Checking for x64 gcc inline assembler"
  ${check_src}
  DIRECT
  )

endif()

#######################################################################
#
# Check for various properties of floating point
#
#######################################################################
# Check whether C doubles are little-endian IEEE 754 binary64
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_little_endian_double.c)
file(WRITE ${check_src} "#include <string.h>
int main() {
    double x = 9006104071832581.0;
    if (memcmp(&x, \"\\x05\\x04\\x03\\x02\\x01\\xff\\x3f\\x43\", 8) == 0)
        return 0;
    else
        return 1;
}
")
python_platform_test_run(
  DOUBLE_IS_LITTLE_ENDIAN_IEEE754
  "Checking whether C doubles are little-endian IEEE 754 binary64"
  ${check_src}
  DIRECT
  )

# Check whether C doubles are big-endian IEEE 754 binary64
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_big_endian_double.c)
file(WRITE ${check_src} "#include <string.h>
int main() {
    double x = 9006104071832581.0;
    if (memcmp(&x, \"\\x43\\x3f\\xff\\x01\\x02\\x03\\x04\\x05\", 8) == 0)
        return 0;
    else
        return 1;
}
")
python_platform_test_run(
  DOUBLE_IS_BIG_ENDIAN_IEEE754
  "Checking whether C doubles are big-endian IEEE 754 binary64"
  ${check_src}
  DIRECT
  )

# Check whether C doubles are ARM mixed-endian IEEE 754 binary64
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_mixed_endian_double.c)
file(WRITE ${check_src} "#include <string.h>
int main() {
    double x = 9006104071832581.0;
    if (memcmp(&x, \"\\x01\\xff\\x3f\\x43\\x05\\x04\\x03\\x02\", 8) == 0)
        return 0;
    else
        return 1;
}
")
python_platform_test_run(
  DOUBLE_IS_ARM_MIXED_ENDIAN_IEEE754
  "Checking doubles are ARM mixed-endian IEEE 754 binary64"
  ${check_src}
  DIRECT
  )

# The short float repr introduced in Python 3.1 requires the
# correctly-rounded string <-> double conversion functions from
# Python/dtoa.c, which in turn require that the FPU uses 53-bit
# rounding; this is a problem on x86, where the x87 FPU has a default
# rounding precision of 64 bits.  For gcc/x86, we can fix this by
# using inline assembler to get and set the x87 FPU control word.

# This inline assembler syntax may also work for suncc and icc,
# so we try it on all platforms.

# Check whether we can use gcc inline assembler to get and set x87 control word
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gcc_asm_for_x87.c)
file(WRITE ${check_src} "int main() {
  unsigned short cw;
  __asm__ __volatile__ (\"fnstcw %0\" : \"=m\" (cw));
  __asm__ __volatile__ (\"fldcw %0\" : : \"m\" (cw));
}
")
python_platform_test(
  HAVE_GCC_ASM_FOR_X87
  "Checking whether we can use gcc inline assembler to get and set x87 control word"
  ${check_src}
  DIRECT
  )

# FIXME: setup some libffi specific variables
# # libffi specific: Cannot use PROT_EXEC on this target, so, we revert to alternative means
# # XXX In autoconf system, it was set to true if target matches *arm*-apple-darwin*
# if(NOT DEFINED FFI_EXEC_TRAMPOLINE_TABLE)
#   set(FFI_EXEC_TRAMPOLINE_TABLE 0)
# endif()

# # libffi specific: Define this if you want to enable pax emulated trampolines
# # On PaX enable kernels that have MPROTECT enable we can't use PROT_EXEC.
# # XXX Add option 'FFI_MMAP_EXEC_EMUTRAMP_PAX'.
# if(NOT DEFINED FFI_MMAP_EXEC_EMUTRAMP_PAX)
#   set(FFI_MMAP_EXEC_EMUTRAMP_PAX 0)
# endif()

# # libffi specific: Check whether read-only mmap of a plain file works
# if(NOT DEFINED HAVE_MMAP_FILE)
#   set(msg "Checking whether read-only mmap of a plain file works")
#   message(STATUS "${msg}")
#   set(value 1)
#   set(status "yes")
#   if(NOT HAVE_SYS_MMAN_H OR NOT HAVE_MMAP)
#     set(value 0)
#     set(status "no")
#   endif()
#   message(STATUS "${msg} - ${status}")
#   set(HAVE_MMAP_FILE "${value}" CACHE INTERNAL "Have support for mmap_file")
# endif()

# # libffi specific: check whether mmap works
# # XXX Add a system to this blacklist if
# #     mmap(0, stat_size, PROT_READ, MAP_PRIVATE, fd, 0) doesn't return a
# #     memory area containing the same data that you'd get if you applied
# #     read() to the same fd.  The only system known to have a problem here
# #     is VMS, where text files have record structure.

# # libffi specific: Check whether assembler supports .ascii.
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_as_ascii_pseudo_op.c)
# file(WRITE ${check_src} "
# int main () {
# asm (\".ascii \\\"string\\\"\");
# return 0;
# }
# ")
# python_platform_test(
#   HAVE_AS_ASCII_PSEUDO_OP
#   "Checking whether assembler supports .ascii"
#   ${check_src}
#   DIRECT
#   )

# # libffi specific: Check whether assembler supports .cfi_* directives
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_as_cfi_pseudo_op.c)
# file(WRITE ${check_src} "
# asm(\".cfi_startproc\\n\\t.cfi_endproc\");
# int main (){return 0;}
# ")
# python_platform_test(
#   HAVE_AS_CFI_PSEUDO_OP
#   "Checking whether assembler supports .cfi_* directives"
#   ${check_src}
#   DIRECT
#   )

# # libffi specific: Check whether assembler supports .string.
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_as_string_pseudo_op.c)
# file(WRITE ${check_src} "
# int main () {
# asm (\".string \\\"string\\\"\");
# return 0;
# }
# ")
# python_platform_test(
#   HAVE_AS_STRING_PSEUDO_OP
#   "Checking whether assembler supports .string"
#   ${check_src}
#   DIRECT
#   )

# # libffi specific: Check whether assembler supports unwind section type.
# # TODO
# set(HAVE_AS_X86_64_UNWIND_SECTION_TYPE 1)

# # libffi specific: Check whether assembler supports PC relative relocs
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_as_x86_pcrel.c)
# file(WRITE ${check_src} "int main() {
#     __asm__ __volatile__ (\".text; ha: nop; .data; .long ha-.; .text\");
# }
# ")
# python_platform_test(
#   HAVE_AS_X86_PCREL
#   "Checking whether assembler supports PC relative relocs"
#   ${check_src}
#   DIRECT
#   )

# # libffi specific: Check whether .eh_frame sections should be read-only.
# # TODO
# set(HAVE_RO_EH_FRAME 1)

# # libffi specific: Check whether symbols are underscored
# # TODO
# set(SYMBOL_UNDERSCORE 0)

# # libffi specific: Check compiler for symbol visibility support
# check_c_source_compiles("
#         __attribute__((visibility(\"default\")))
#         int bar(void) {};
#         int main() {bar();}"
#         HAVE_HIDDEN_VISIBILITY_ATTRIBUTE)

# # libffi specific: Check system for MAP_ANONYMOUS
# check_c_source_compiles("
# #include <sys/types.h>
# #include <sys/mman.h>
# #include <unistd.h>

# #ifndef MAP_ANONYMOUS
# #define MAP_ANONYMOUS MAP_ANON
# #endif

# int main() {int a = MAP_ANONYMOUS;}"
# HAVE_MMAP_ANON)

# # libffi specific: Check for /dev/zero support for anonymous memory maps
# check_c_source_runs("
# #include <stdlib.h>
# #include <sys/types.h>
# #include <sys/mman.h>
# #include <fcntl.h>
# int main(void) {
#   int devzero;
#   void *retval;
#   devzero = open(\"/dev/zero\", O_RDWR);
#   if (-1 == devzero) {
#     exit(1);
#   }
#   retval = mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, devzero, 0);
#   if (retval == (void *)-1) {
#     exit(1);
#   }
#   exit(0);
# }" HAVE_MMAP_DEV_ZERO)

if(IS_PY3)

# Check whether we can use gcc inline assembler to get and set mc68881 fpcr
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gcc_asm_for_mc68881.c)
file(WRITE ${check_src} "int main() {
  unsigned int fpcr;
  __asm__ __volatile__ (\"fmove.l %%fpcr,%0\" : \"=g\" (fpcr));
  __asm__ __volatile__ (\"fmove.l %0,%%fpcr\" : : \"g\" (fpcr));
}
")
python_platform_test(
  HAVE_GCC_ASM_FOR_MC68881
  "Checking whether we can use gcc inline assembler to get and set mc68881 fpcr"
  ${check_src}
  DIRECT
  )

endif()

# Check for x87-style double rounding
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_x87_double_rounding.c)
file(WRITE ${check_src} "#include <stdlib.h>
#include <math.h>
int main() {
    volatile double x, y, z;
    /* 1./(1-2**-53) -> 1+2**-52 (correct), 1.0 (double rounding) */
    x = 0.99999999999999989; /* 1-2**-53 */
    y = 1./x;
    if (y != 1.)
        exit(0);
    /* 1e16+2.99999 -> 1e16+2. (correct), 1e16+4. (double rounding) */
    x = 1e16;
    y = 2.99999;
    z = x + y;
    if (z != 1e16+4.)
        exit(0);
    /* both tests show evidence of double rounding */
    exit(1);
}
")
python_platform_test_run(
  X87_DOUBLE_ROUNDING
  "Checking for x87-style double rounding"
  ${check_src}
  INVERT
  )

#######################################################################
#
# Check for crypt functions
#
#######################################################################
# FIXME: check crypt functions

# We search for both crypt and crypt_r as one or the other may be defined
# This gets us our -lcrypt in CMAKE_REQUIRED_LIBRARIES when required on
# the target platform.

# cmake_push_check_state()
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_crypt_crypt.c)
# file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
# Use char because int might match the return type of a GCC
# builtin and then its argument prototype would still apply.  */
# #ifdef __cplusplus
# extern \"C\"
# #endif
# char crypt ();
# int main () { return crypt (); }
# ")
# list(APPEND CMAKE_REQUIRED_LIBRARIES crypt)
# python_platform_test(
#   HAVE_CRYPT
#   "Checking for crypt in -lcrypt"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()

# cmake_push_check_state()
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_crypt_crypt_r.c)
# file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
# Use char because int might match the return type of a GCC
# builtin and then its argument prototype would still apply.  */
# #ifdef __cplusplus
# extern \"C\"
# #endif
# char crypt_r ();
# int main () { return crypt_r (); }
# ")
# list(APPEND CMAKE_REQUIRED_LIBRARIES crypt)
# python_platform_test(
#   HAVE_CRYPT
#   "Checking for crypt_r in -lcrypt"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()

# add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_CRYPT crypt)

# if(HAVE_CRYPT_H)
#   check_c_source_compiles("
# #define _GNU_SOURCE  /* Required for crypt_r()'s prototype in glibc. */
# #include <crypt.h>
# int main(void){
#   struct crypt_data d;
#   char *r = crypt_r(\"\", \"\", &d); }" HAVE_CRYPT_R)
# endif()

#######################################################################
#
# Check for mathematical functions
#
#######################################################################
cmake_push_check_state()

# Check whether tanh preserves the sign of zero
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_tanh_preserves_zero_sign.c)
file(WRITE ${check_src} "#include <math.h>
#include <stdlib.h>
int main() {
    /* return 0 if either negative zeros don't exist
       on this platform or if negative zeros exist
       and tanh(-0.) == -0. */
  if (atan2(0., -1.) == atan2(-0., -1.) ||
      atan2(tanh(-0.), -1.) == atan2(-0., -1.)) exit(0);
  else exit(1);
}
")
python_platform_test_run(
  TANH_PRESERVES_ZERO_SIGN
  "Checking whether tanh preserves the sign of zero"
  ${check_src}
  DIRECT
  )

if(IS_PY3)
# Check whether log1p drops the sign of negative zero
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_log1p_drops_zero_sign.c)
file(WRITE ${check_src} "#include <math.h>
#include <stdlib.h>
int main() {
  /* Fail if the signs of log1p(-0.) and -0. can be distinguished. */
  if (atan2(log1p(-0.), -1.) == atan2(-0., -1.))
      return 0;
  else
      return 1;
}
")
python_platform_test_run(
  LOG1P_DROPS_ZERO_SIGN
  "Checking whether log1p drops the sign of negative zero"
  ${check_src}
  INVERT
  )
endif()

set(_funcs acosh asinh atanh copysign erf erfc expm1 finite gamma
  hypot lgamma log1p round tgamma
  )
if(IS_PY3)
  list(APPEND _funcs log2)
endif()
foreach(func ${_funcs})
  string(TOUPPER ${func} _func_upper)
  check_function_exists(${func} HAVE_${_func_upper})
endforeach()

foreach(decl isinf isnan isfinite)
  string(TOUPPER ${decl} _decl_upper)
  check_symbol_exists(${decl} "math.h" HAVE_DECL_${_decl_upper})
endforeach()

cmake_pop_check_state()

#######################################################################
#
# time
#
#######################################################################
check_struct_has_member("struct tm"   tm_zone    "${CFG_HEADERS}"    HAVE_STRUCT_TM_TM_ZONE)
check_struct_has_member("struct tm"   tm_zone    "${CFG_HEADERS}"    HAVE_STRUCT_TM_TM_ZONE)
set(HAVE_TM_ZONE ${HAVE_STRUCT_TM_TM_ZONE})

if(NOT HAVE_STRUCT_TM_TM_ZONE)
  check_variable_exists(tzname HAVE_TZNAME)
  check_symbol_exists(tzname "time.h" HAVE_DECL_TZNAME)
else()
  set(HAVE_TZNAME 0)
  set(HAVE_DECL_TZNAME 0)
endif()

set(CMAKE_EXTRA_INCLUDE_FILES "time.h")
check_type_size("struct tm" TM_IN_TIME)
unset(CMAKE_EXTRA_INCLUDE_FILES)
if(TM_IN_TIME)
  set(TM_IN_SYS_TIME 0)
else()
  set(TM_IN_SYS_TIME 1)
endif()
check_c_source_compiles("#include <sys/types.h>\n #include <sys/time.h>\n #include <time.h>\n int main() {if ((struct tm *) 0) return 0;}" TIME_WITH_SYS_TIME)
check_c_source_compiles("#include <sys/types.h>\n #include <sys/select.h>\n #include <sys/time.h>\n int main(){return 0;}" SYS_SELECT_WITH_SYS_TIME)

check_c_source_compiles("#include <sys/time.h>\n int main() {gettimeofday((struct timeval*)0,(struct timezone*)0);}" GETTIMEOFDAY_WITH_TZ)

if(GETTIMEOFDAY_WITH_TZ)
  set(GETTIMEOFDAY_NO_TZ 0)
else()
  set(GETTIMEOFDAY_NO_TZ 1)
endif()

if(IS_PY3)

if(APPLE)
  cmake_push_check_state()
  set(CMAKE_REQUIRED_FLAGS "-Wl,-no_weak_imports")
  CHECK_C_SOURCE_COMPILES("int main(void) { return 0; }" SUPPORT_NO_WEAK_IMPORT_FLAG)
  cmake_pop_check_state()
else()
  set(SUPPORT_NO_WEAK_IMPORT_FLAG 0)
endif()

cmake_push_check_state()
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/clock_getres.c)
file(WRITE ${check_src} "
#include <stdio.h>
#include <time.h>
int main() { return clock_getres(0, NULL); }
")
if(SUPPORT_NO_WEAK_IMPORT_FLAG)
  set(CMAKE_REQUIRED_FLAGS "-Wl,-no_weak_imports")
endif()
python_platform_test(
  HAVE_CLOCK_GETRES
  "Checking for clock_getres"
  ${check_src}
  DIRECT
  )
cmake_pop_check_state()

if(NOT HAVE_CLOCK_GETRES)
  cmake_push_check_state()
  set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_rt_clock_getres.c)
  file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
  Use char because int might match the return type of a GCC
  builtin and then its argument prototype would still apply.  */
  #ifdef __cplusplus
  extern \"C\"
  #endif
  char clock_getres ();
  int main () { return clock_getres (); }
  ")
  list(APPEND CMAKE_REQUIRED_LIBRARIES rt)
  python_platform_test(
    HAVE_CLOCK_GETRES
    "Checking for clock_getres in -lrt"
    ${check_src}
    DIRECT
    )
  cmake_pop_check_state()
endif()

cmake_push_check_state()
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/clock_gettime.c)
file(WRITE ${check_src} "
#include <stdio.h>
#include <time.h>
int main() { return clock_gettime(0, NULL); }
")
if(SUPPORT_NO_WEAK_IMPORT_FLAG)
  set(CMAKE_REQUIRED_FLAGS "-Wl,-no_weak_imports")
endif()
python_platform_test(
  HAVE_CLOCK_GETTIME
  "Checking for clock_gettime"
  ${check_src}
  DIRECT
  )
cmake_pop_check_state()
if(NOT HAVE_CLOCK_GETTIME)
  cmake_push_check_state()
  set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_rt_clock_gettime.c)
  file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
    Use char because int might match the return type of a GCC
    builtin and then its argument prototype would still apply.  */
    #ifdef __cplusplus
    extern \"C\"
    #endif
    char clock_gettime ();
    int main () { return clock_gettime (); }
  ")
  list(APPEND CMAKE_REQUIRED_LIBRARIES rt)
  python_platform_test(
    HAVE_CLOCK_GETTIME
    "Checking for clock_gettime in -lrt"
    ${check_src}
    DIRECT
    )
  cmake_pop_check_state()
  if(HAVE_CLOCK_GETTIME)
    set(TIMEMODULE_LIB rt)
  endif()
endif()

cmake_push_check_state()
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/clock_settime.c)
file(WRITE ${check_src} "
#include <stdio.h>
#include <time.h>
int main() { return clock_settime(0, NULL); }
")
if(SUPPORT_NO_WEAK_IMPORT_FLAG)
  set(CMAKE_REQUIRED_FLAGS "-Wl,-no_weak_imports")
endif()
python_platform_test(
  HAVE_CLOCK_SETTIME
  "Checking for clock_settime"
  ${check_src}
  DIRECT
  )
cmake_pop_check_state()
if(NOT HAVE_CLOCK_SETTIME)
  cmake_push_check_state()
  set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_rt_clock_settime.c)
  file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
    Use char because int might match the return type of a GCC
    builtin and then its argument prototype would still apply.  */
    #ifdef __cplusplus
    extern \"C\"
    #endif
    char clock_gettime ();
    int main () { return clock_settime (); }
  ")
  list(APPEND CMAKE_REQUIRED_LIBRARIES rt)
  python_platform_test(
    HAVE_CLOCK_SETTIME
    "Checking for clock_settime in -lrt"
    ${check_src}
    DIRECT
    )
  cmake_pop_check_state()
  if(HAVE_CLOCK_SETTIME)
    set(TIMEMODULE_LIB rt)
  endif()
endif()

endif()

#######################################################################
#
# unicode
#
#######################################################################

# Check for UCS-4 tcl
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_ucs4_tcl.c)
file(WRITE ${check_src} "#include <tcl.h>
#if TCL_UTF_MAX != 6
# error \"NOT UCS4_TCL\"
#endif
int main () { return 0; }
")
python_platform_test(
  HAVE_UCS4_TCL
  "Checking for UCS-4 tcl"
  ${check_src}
  DIRECT
  )

# #ucs2
# set(HAVE_USABLE_WCHAR_T 0)

# if(PY_VERSION VERSION_GREATER_EQUAL "3.7")
# set(PY_COERCE_C_LOCALE ${WITH_C_LOCALE_COERCION})
# endif()

#######################################################################
#
# uuid tests
#
#######################################################################
# FIXME: we don't enable uuid extension for now, check if it works after enable it

if(PY_VERSION VERSION_GREATER_EQUAL "3.7")

cmake_push_check_state()
set(CFG_HEADERS_SAVE ${CFG_HEADERS})

check_include_files(uuid/uuid.h HAVE_UUID_UUID_H)
check_include_files(uuid.h HAVE_UUID_H)

add_cond(CFG_HEADERS HAVE_UUID_UUID_H uuid/uuid.h)
add_cond(CFG_HEADERS HAVE_UUID_H uuid.h)

# add_cond(CMAKE_REQUIRED_LIBRARIES UUID_LIBRARY ${UUID_LIBRARY})

check_symbol_exists(uuid_create "${CFG_HEADERS}" HAVE_UUID_CREATE)

if(HAVE_UUID_H)
  # Little-endian FreeBSD, OpenBSD and NetBSD needs encoding into an octet
  # stream in big-endian byte-order
  check_c_source_compiles("#include <uuid.h>
int main(void){
  #ifndef uuid_enc_be
  uuid_t uuid;
  unsigned char buf[sizeof(uuid)];
  uuid_enc_be(buf, &uuid);
  #endif
}" HAVE_UUID_ENC_BE)
endif()

if(HAVE_UUID_UUID_H)
  check_c_source_compiles("#include <uuid/uuid.h>
int main(void){
  #ifndef uuid_generate_time_safe
  uuid_t out;
  uuid_generate_time_safe(out);
  #endif
}" HAVE_UUID_GENERATE_TIME_SAFE)
endif()

set(CFG_HEADERS ${CFG_HEADERS_SAVE})
cmake_pop_check_state()

endif()

#######################################################################
#
# networking tests
#
#######################################################################
cmake_push_check_state()
set(CFG_HEADERS_SAVE ${CFG_HEADERS})
add_cond(CFG_HEADERS HAVE_NETDB_H netdb.h)
add_cond(CFG_HEADERS HAVE_NETINET_IN_H netinet/in.h)
add_cond(CFG_HEADERS HAVE_ARPA_INET_H arpa/inet.h)

check_symbol_exists(gethostbyname_r "${CFG_HEADERS}" HAVE_GETHOSTBYNAME_R)
if(HAVE_GETHOSTBYNAME_R)
  # Checking gethostbyname_r with 6 args
  set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gethostbyname_r_6_arg.c)
  file(WRITE ${check_src} "
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main() {
    char *name;
    struct hostent *he, *res;
    char buffer[2048];
    int buflen = 2048;
    int h_errnop;

    (void) gethostbyname_r(name, he, buffer, buflen, &res, &h_errnop);
    return 0;
}
")

  python_platform_test(
    HAVE_GETHOSTBYNAME_R_6_ARG
    "Checking gethostbyname_r with 6 args"
    ${check_src}
    DIRECT
    )
  if(HAVE_GETHOSTBYNAME_R_6_ARG)
    set(HAVE_GETHOSTBYNAME_R 1)
  else()
    message(FATAL_ERROR "gethostbyname_r with 6 args is not supported")
    # Checking gethostbyname_r with 5 args
    set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gethostbyname_r_5_arg.c)
    file(WRITE ${check_src} "int main() {
    char *name;
    struct hostent *he;
    char buffer[2048];
    int buflen = 2048;
    int h_errnop;

    (void) gethostbyname_r(name, he, buffer, buflen, &h_errnop)
    return 0;
}
")
    python_platform_test(
      HAVE_GETHOSTBYNAME_R_5_ARG
      "Checking gethostbyname_r with 5 args"
      ${check_src}
      DIRECT
      )
    if(HAVE_GETHOSTBYNAME_R_5_ARG)
      set(HAVE_GETHOSTBYNAME_R 1)
    else()
      # Checking gethostbyname_r with 5 args
      set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_gethostbyname_r_3_arg.c)
      file(WRITE ${check_src} "int main() {
    char *name;
    struct hostent *he;
    struct hostent_data data;

    (void) gethostbyname_r(name, he, &data);
    return 0;
}
")
      python_platform_test(
        HAVE_GETHOSTBYNAME_R_3_ARG
        "Checking gethostbyname_r with 3 args"
        ${check_src}
        DIRECT
        )
      if(HAVE_GETHOSTBYNAME_R_3_ARG)
        set(HAVE_GETHOSTBYNAME_R 1)
      endif()
    endif()
  endif()
else()
  check_symbol_exists(gethostbyname   "${CFG_HEADERS}" HAVE_GETHOSTBYNAME)
endif()

check_symbol_exists(gai_strerror    "${CFG_HEADERS}" HAVE_GAI_STRERROR)
check_symbol_exists(getaddrinfo     "${CFG_HEADERS}" HAVE_GETADDRINFO)
check_symbol_exists(getnameinfo     "${CFG_HEADERS}" HAVE_GETNAMEINFO)
check_symbol_exists(getpeername     "${CFG_HEADERS}" HAVE_GETPEERNAME)
check_symbol_exists(hstrerror       "${CFG_HEADERS}" HAVE_HSTRERROR)
check_symbol_exists(inet_aton       "${CFG_HEADERS}" HAVE_INET_ATON)
if(NOT HAVE_INET_ATON)
  check_library_exists(resolv inet_aton "" HAVE_LIBRESOLV)
endif()
check_symbol_exists(inet_pton       "${CFG_HEADERS}" HAVE_INET_PTON)

set(CMAKE_EXTRA_INCLUDE_FILES ${CFG_HEADERS})
check_type_size("struct addrinfo" HAVE_ADDRINFO)
check_struct_has_member("struct sockaddr" sa_len "${CFG_HEADERS}" HAVE_SOCKADDR_SA_LEN )
check_type_size("struct sockaddr_storage" HAVE_SOCKADDR_STORAGE)
unset(CMAKE_EXTRA_INCLUDE_FILES)

set(CMAKE_EXTRA_INCLUDE_FILES "sys/types.h" "sys/socket.h" "linux/if_alg.h")
check_type_size("struct sockaddr_alg" HAVE_SOCKADDR_ALG)
unset(CMAKE_EXTRA_INCLUDE_FILES)

set(CFG_HEADERS ${CFG_HEADERS_SAVE})
cmake_pop_check_state()


#######################################################################
#
# multithreading stuff
#
#######################################################################
cmake_push_check_state()
set(CFG_HEADERS_SAVE ${CFG_HEADERS})

set(HAVE_PTHREAD_DESTRUCTOR 0) # for Solaris 2.6
add_cond(CFG_HEADERS  HAVE_PTHREAD_H  pthread.h)
add_cond(CMAKE_REQUIRED_LIBRARIES  CMAKE_USE_PTHREADS_INIT  "${CMAKE_THREAD_LIBS_INIT}")
if(APPLE)
  set(HAVE_PTHREAD_INIT ${CMAKE_USE_PTHREADS_INIT}) # See commit message for explanation.
else()
  check_symbol_exists(pthread_init "${CFG_HEADERS}" HAVE_PTHREAD_INIT)
endif()
check_symbol_exists(pthread_sigmask "${CFG_HEADERS}" HAVE_PTHREAD_SIGMASK)
check_symbol_exists(pthread_atfork "${CFG_HEADERS}" HAVE_PTHREAD_ATFORK)
check_symbol_exists(pthread_condattr_setclock "${CFG_HEADERS}" HAVE_PTHREAD_CONDATTR_SETCLOCK)
check_symbol_exists(pthread_getcpuclockid "${CFG_HEADERS}" HAVE_PTHREAD_GETCPUCLOCKID)

add_cond(CFG_HEADERS  HAVE_SEMAPHORE_H  semaphore.h)
check_symbol_exists(sem_clockwait "${CFG_HEADERS}" HAVE_SEM_CLOCKWAIT)
check_symbol_exists(sem_getvalue "${CFG_HEADERS}" HAVE_SEM_GETVALUE)
check_symbol_exists(sem_open "${CFG_HEADERS}" HAVE_SEM_OPEN)
check_symbol_exists(sem_timedwait "${CFG_HEADERS}" HAVE_SEM_TIMEDWAIT)
check_symbol_exists(sem_unlink "${CFG_HEADERS}" HAVE_SEM_UNLINK)

# For multiprocessing module, check that sem_open actually works.
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_posix_semaphores_enabled.c)
file(WRITE ${check_src} "#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <semaphore.h>
#include <sys/stat.h>

int main(void) {
  sem_t *a = sem_open(\"/autoconf\", O_CREAT, S_IRUSR|S_IWUSR, 0);
  if (a == SEM_FAILED) {
    perror(\"sem_open\");
    return 1;
  }
  sem_close(a);
  sem_unlink(\"/autoconf\");
  return 0;
}
")
python_platform_test_run(
  POSIX_SEMAPHORES_NOT_ENABLED
  "Checking whether POSIX semaphores are enabled"
  ${check_src}
  INVERT
  )

# Multiprocessing check for broken sem_getvalue
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_broken_sem_getvalue.c)
file(WRITE ${check_src} "#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <semaphore.h>
#include <sys/stat.h>

int main(void){
  sem_t *a = sem_open(\"/autocftw\", O_CREAT, S_IRUSR|S_IWUSR, 0);
  int count;
  int res;
  if(a==SEM_FAILED){
    perror(\"sem_open\");
    return 1;

  }
  res = sem_getvalue(a, &count);
  sem_close(a);
  sem_unlink(\"/autocftw\");
  return res==-1 ? 1 : 0;
}
")
python_platform_test_run(
  HAVE_BROKEN_SEM_GETVALUE
  "Checking for broken sem_getvalue"
  ${check_src}
  INVERT
  )

set(CFG_HEADERS ${CFG_HEADERS_SAVE})
cmake_pop_check_state()

if(CMAKE_SYSTEM MATCHES BlueGene AND PY_VERSION VERSION_LESS "3.7")
  # Todo: Display message
  set(WITH_THREAD OFF CACHE STRING "System doesn't support multithreading" FORCE)
endif()


#######################################################################
#
# ssl tests
#
#######################################################################

# FIXME: check ssl function
# if(OPENSSL_INCLUDE_DIR AND OPENSSL_LIBRARIES)
#   cmake_push_check_state()

#   set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
#   set(CMAKE_REQUIRED_LIBRARIES ${OPENSSL_LIBRARIES})

#   # Checking for X509_VERIFY_PARAM_set1_host in libssl
#   check_c_source_compiles("#include <openssl/x509_vfy.h>
# int main(void){
#   X509_VERIFY_PARAM *p = X509_VERIFY_PARAM_new();
#   X509_VERIFY_PARAM_set1_host(p, \"localhost\", 0);
#   X509_VERIFY_PARAM_set1_ip_asc(p, \"127.0.0.1\");
#   X509_VERIFY_PARAM_set_hostflags(p, 0); }" HAVE_X509_VERIFY_PARAM_SET1_HOST)

#   cmake_pop_check_state()
# endif()


#######################################################################
#
# readline tests
#
#######################################################################
# FIXME: check readline function
# # MacOSX 10.4 has a broken readline. Don't try to build
# # the readline module unless the user has installed a fixed
# # readline package
# if(HAVE_READLINE_READLINE_H)
#   if(APPLE AND CMAKE_OSX_DEPLOYMENT_TARGET
#      AND "${CMAKE_OSX_DEPLOYMENT_TARGET}" VERSION_LESS "10.5")
#     check_include_files(readline/rlconf.h HAVE_READLINE_RLCONF_H)
#     if(NOT HAVE_READLINE_RLCONF_H)
#       set(HAVE_READLINE_READLINE_H FALSE)
#     endif()
#   endif()
# endif()
# if(HAVE_READLINE_READLINE_H)
#   cmake_push_check_state()
#   set(CFG_HEADERS_SAVE ${CFG_HEADERS})

#   add_cond(CFG_HEADERS HAVE_READLINE_READLINE_H readline/readline.h)
#   add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBREADLINE ${HAVE_LIBREADLINE})
#   check_symbol_exists(rl_callback_handler_install "${CFG_HEADERS}" HAVE_RL_CALLBACK)
#   check_symbol_exists(rl_catch_signals            "${CFG_HEADERS}" HAVE_RL_CATCH_SIGNAL)
#   check_symbol_exists(rl_completion_append_character     "${CFG_HEADERS}" HAVE_RL_COMPLETION_APPEND_CHARACTER)
#   check_symbol_exists(rl_completion_display_matches_hook "${CFG_HEADERS}" HAVE_RL_COMPLETION_DISPLAY_MATCHES_HOOK)
#   check_symbol_exists(rl_completion_suppress_append      "${CFG_HEADERS}" HAVE_RL_COMPLETION_SUPPRESS_APPEND)
#   check_symbol_exists(rl_completion_matches       "${CFG_HEADERS}" HAVE_RL_COMPLETION_MATCHES)
#   check_symbol_exists(rl_pre_input_hook           "${CFG_HEADERS}" HAVE_RL_PRE_INPUT_HOOK)
#   check_symbol_exists(rl_resize_terminal          "${CFG_HEADERS}" HAVE_RL_RESIZE_TERMINAL)

#   set(CFG_HEADERS ${CFG_HEADERS_SAVE})
#   cmake_pop_check_state()
# endif()


#######################################################################
#
# curses tests
#
#######################################################################
# FIXME: check curses function
# if(HAVE_CURSES_H)
#   cmake_push_check_state()
#   set(CFG_HEADERS_SAVE ${CFG_HEADERS})

#   set(CFG_HEADERS ${CFG_HEADERS} curses.h)
#   add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBCURSES ${HAVE_LIBCURSES})
#   check_symbol_exists(is_term_resized "${CFG_HEADERS}" HAVE_CURSES_IS_TERM_RESIZED)
#   check_symbol_exists(resizeterm      "${CFG_HEADERS}" HAVE_CURSES_RESIZETERM)
#   check_symbol_exists(resize_term     "${CFG_HEADERS}" HAVE_CURSES_RESIZE_TERM)
#   check_struct_has_member(WINDOW _flags   "${CFG_HEADERS}" WINDOW_HAS_FLAGS)

#   check_c_source_compiles("#include <curses.h>\n int main() {int i; i = mvwdelch(0,0,0);}" MVWDELCH_IS_EXPRESSION)

#   check_symbol_exists(filter "${CFG_HEADERS}" HAVE_CURSES_FILTER)
#   check_symbol_exists(has_key "${CFG_HEADERS}" HAVE_CURSES_HAS_KEY)
#   check_symbol_exists(immedok "${CFG_HEADERS}" HAVE_CURSES_IMMEDOK)
#   check_symbol_exists(is_pad "${CFG_HEADERS}" HAVE_CURSES_IS_PAD)
#   check_symbol_exists(syncok "${CFG_HEADERS}" HAVE_CURSES_SYNCOK)
#   check_symbol_exists(typeahead "${CFG_HEADERS}" HAVE_CURSES_TYPEAHEAD)
#   check_symbol_exists(use_env "${CFG_HEADERS}" HAVE_CURSES_USE_ENV)
#   check_symbol_exists(wchgat "${CFG_HEADERS}" HAVE_CURSES_WCHGAT)

#   set(CFG_HEADERS ${CFG_HEADERS_SAVE})
#   cmake_pop_check_state()
# endif()


#######################################################################
#
# dynamic loading
#
#######################################################################
if(HAVE_DLFCN_H)
  cmake_push_check_state()
  set(CFG_HEADERS_SAVE ${CFG_HEADERS})

  set(CFG_HEADERS ${CFG_HEADERS} dlfcn.h)
  add_cond(CMAKE_REQUIRED_LIBRARIES HAVE_LIBDL "${HAVE_LIBDL}")
  check_symbol_exists(dlopen          "${CFG_HEADERS}" HAVE_DLOPEN)

  check_symbol_exists(RTLD_DEEPBIND dlfcn.h HAVE_DECL_RTLD_DEEPBIND)
  check_symbol_exists(RTLD_GLOBAL   dlfcn.h HAVE_DECL_RTLD_GLOBAL)
  check_symbol_exists(RTLD_LAZY     dlfcn.h HAVE_DECL_RTLD_LAZY)
  check_symbol_exists(RTLD_LOCAL    dlfcn.h HAVE_DECL_RTLD_LOCAL)
  check_symbol_exists(RTLD_MEMBER   dlfcn.h HAVE_DECL_RTLD_MEMBER)
  check_symbol_exists(RTLD_NODELETE dlfcn.h HAVE_DECL_RTLD_NODELETE)
  check_symbol_exists(RTLD_NOLOAD   dlfcn.h HAVE_DECL_RTLD_NOLOAD)
  check_symbol_exists(RTLD_NOW      dlfcn.h HAVE_DECL_RTLD_NOW)

  set(CFG_HEADERS ${CFG_HEADERS_SAVE})
  cmake_pop_check_state()
endif()

if(HAVE_DLOPEN) # OR .... )
  set(HAVE_DYNAMIC_LOADING 1)
else()
  set(HAVE_DYNAMIC_LOADING 0)
endif()


#######################################################################
#
# check some types
#
#######################################################################
check_type_size("gid_t" HAVE_GID_T)
if(NOT HAVE_GID_T)
  set(gid_t int)
else()
  set(gid_t 0)
endif()

check_type_size("mode_t" HAVE_MODE_T)
if(NOT HAVE_MODE_T)
  set(mode_t int)
else()
  set(mode_t 0)
endif()

check_type_size("off_t" HAVE_OFF_T)
if(NOT HAVE_OFF_T)
  set(off_t "long int")
else()
  set(off_t 0)
endif()

check_type_size("pid_t" HAVE_PID_T)
if(NOT HAVE_PID_T)
  set(pid_t int)
else()
  set(pid_t 0)
endif()

check_type_size("size_t" HAVE_SIZE_T)
if(NOT HAVE_SIZE_T)
  set(size_t "unsigned int")
else()
  set(size_t 0)
endif()

set(CMAKE_EXTRA_INCLUDE_FILES ${CFG_HEADERS})
check_type_size("uint64_t" HAVE_UINT64_T)
check_type_size("int64_t" HAVE_INT64_T)
check_type_size("uint32_t" HAVE_UINT32_T)
check_type_size("int32_t" HAVE_INT32_T)
if(IS_PY3)
check_type_size("__uint128_t" HAVE_GCC_UINT128_T)
endif()
unset(CMAKE_EXTRA_INCLUDE_FILES)

set(CMAKE_EXTRA_INCLUDE_FILES "sys/socket.h")
check_type_size("socklen_t" HAVE_SOCKLEN_T)
unset(CMAKE_EXTRA_INCLUDE_FILES)
if(NOT HAVE_SOCKLEN_T)
  set(socklen int)
else()
  set(socklen_t 0)
endif()

check_type_size("uid_t" HAVE_UID_T)
if(NOT HAVE_UID_T)
  set(uid_t int)
else()
  set(uid_t 0)
endif()

set(CMAKE_EXTRA_INCLUDE_FILES "time.h")
check_type_size("clock_t" HAVE_CLOCK_T)
unset(CMAKE_EXTRA_INCLUDE_FILES)
if(NOT HAVE_CLOCK_T)
  set(clock_t long)
else()
  set(clock_t 0)
endif()


check_c_source_compiles("int main() {const int i;}" const_WORKS)
if(NOT const_WORKS)
  set(const 1)
else()
  set(const 0)
endif()

check_c_source_compiles("int main() {signed int i;}" signed_WORKS)
if(NOT signed_WORKS)
  set(signed 1)
else()
  set(signed 0)
endif()

check_c_source_compiles("int main() {volatile int i;}" volatile_WORKS)
if(NOT volatile_WORKS)
  set(volatile 1)
else()
  set(volatile 0)
endif()


# FIXME: check prototypes function
# Check for prototypes
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_prototypes.c)
# file(WRITE ${check_src} "int foo(int x) { return 0; } int main() { return foo(10); }")
# python_platform_test(
#   HAVE_PROTOTYPES
#   "Checking for prototypes"
#   ${check_src}
#   DIRECT
#   )

# if(HAVE_STDARG_PROTOTYPES)
#    set(vaargsHeader "stdarg.h")
# else()
#    set(vaargsHeader "varargs.h")
# endif()
# check_c_source_compiles("#include <${vaargsHeader}>\n int main() {va_list list1, list2; list1 = list2;}" NOT_VA_LIST_IS_ARRAY)
# if(NOT_VA_LIST_IS_ARRAY)
#   set(VA_LIST_IS_ARRAY 0)
# else()
#   set(VA_LIST_IS_ARRAY 1)
# endif()

# FIXME: check if char is unsigned
# Check whether char is unsigned
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_c_char_unsigned.c)
# file(WRITE ${check_src} "int main() { static int test_array [1 - 2 * !( ((char) -1) < 0 )];
# test_array [0] = 0; return test_array [0]; return 0; }")
# python_platform_test(
#   HAVE_C_CHAR_UNSIGNED
#   "Checking whether char is unsigned"
#   ${check_src}
#   INVERT
#   )
# if(HAVE_C_CHAR_UNSIGNED AND NOT CMAKE_C_COMPILER_ID MATCHES "^GNU$")
#   set(__CHAR_UNSIGNED__ 1)
# endif()

if(IS_PY3)

# FIXME: check if the dirent structure of a d_type field and DT_UNKNOWN is defined
# # Check if the dirent structure of a d_type field and DT_UNKNOWN is defined
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_dirent_d_type.c)
# file(WRITE ${check_src} "#include <dirent.h>
# int main() {
#   struct dirent entry;
#   return entry.d_type == DT_UNKNOWN;
# }
# ")
# python_platform_test(
#   HAVE_DIRENT_D_TYPE
#   "Checking if the dirent structure of a d_type field"
#   ${check_src}
#   DIRECT
#   )

# FIXME: check getrandom function
# # Check if the Linux getrandom() syscall is available
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_getrandom_syscall.c)
# file(WRITE ${check_src} "#include <sys/syscall.h>
# int main() {
#   char buffer[1];
#   const size_t buflen = sizeof(buffer);
#   const int flags = 0;
#   /* ignore the result, Python checks for ENOSYS at runtime */
#   (void)syscall(SYS_getrandom, buffer, buflen, flags);
#   return 0;
# }
# ")
# python_platform_test(
#   HAVE_GETRANDOM_SYSCALL
#   "Checking for the Linux getrandom() syscall"
#   ${check_src}
#   DIRECT
#   )

# # check if the getrandom() function is available
# # the test was written for the Solaris function of <sys/random.h>
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_getrandom.c)
# file(WRITE ${check_src} "#include <sys/random.h>
# int main() {
#   char buffer[1];
#   const size_t buflen = sizeof(buffer);
#   const int flags = 0;
#   /* ignore the result, Python checks for ENOSYS at runtime */
#   (void)getrandom(buffer, buflen, flags);
#   return 0;
# }
# ")
# python_platform_test(
#   HAVE_GETRANDOM
#   "Checking for the getrandom() function"
#   ${check_src}
#   DIRECT
#   )

endif()

#######################################################################
#
# tests for bugs and other stuff
#
#######################################################################
# FIXME: tests for bugs and other stuff
# if(IS_PY2)
# check_c_source_compiles("
#         void f(char*,...)__attribute((format(PyArg_ParseTuple, 1, 2))) {};
#         int main() {f(NULL);} "
#         HAVE_ATTRIBUTE_FORMAT_PARSETUPLE)
# endif()

# check_c_source_compiles("#include <unistd.h>\n int main() {getpgrp(0);}" GETPGRP_HAVE_ARG)

# check_c_source_compiles("#include <unistd.h>\n int main() {setpgrp(0, 0);}" SETPGRP_HAVE_ARG)

# if(IS_PY3)
# # Check for inline
# set(USE_INLINE 0)
# foreach(inline_type inline __inline__ __inline)
#   set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_c_${inline_type}.c)
#   file(WRITE ${check_src} "#ifndef __cplusplus
#   typedef int foo_t;
#   static ${inline_type} foo_t static_foo () {return 0; }
#   ${inline_type} foo_t foo () {return 0; }
#   int main() { return 0; }
#   #endif
#   ")
#   python_platform_test(
#     HAVE_${inline_type}
#     "Checking for ${inline_type}"
#     ${check_src}
#     DIRECT
#     )
#   if(HAVE_${inline_type})
#     set(USE_INLINE 1)
#     break()
#   endif()
# endforeach()
# if(USE_INLINE AND NOT inline_type STREQUAL "inline")
#   # XXX Need to define <inline_type> as inline
# endif()

# # Check for append_history in -lreadline
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_lib_readline_append_history.c)
# file(WRITE ${check_src} "/* Override any GCC internal prototype to avoid an error.
#   Use char because int might match the return type of a GCC
#   builtin and then its argument prototype would still apply.  */
# #ifdef __cplusplus
# extern \"C\"
# #endif
# char append_history ();
# int main () { return append_history (); }
# ")
# cmake_push_check_state()
# list(APPEND CMAKE_REQUIRED_LIBRARIES readline)
# python_platform_test(
#   HAVE_RL_APPEND_HISTORY
#   "Checking for append_history in -lreadline"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()
# endif()

# check_c_source_runs("#include <unistd.h>\n int main() {
#         int val1 = nice(1);
#         if (val1 != -1 && val1 == nice(2)) exit(0);
#         exit(1);}" HAVE_BROKEN_NICE)

# check_c_source_runs(" #include <poll.h>
#     int main () {
#     struct pollfd poll_struct = { 42, POLLIN|POLLPRI|POLLOUT, 0 }; close (42);
#     int poll_test = poll (&poll_struct, 1, 0);
#     if (poll_test < 0) { exit(0); }
#     else if (poll_test == 0 && poll_struct.revents != POLLNVAL) { exit(0); }
#     else { exit(1); } }"
#     HAVE_BROKEN_POLL)

# # Check tzset(3) exists and works like we expect it to
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_working_tzset.c)
# file(WRITE ${check_src} "#include <stdlib.h>
# #include <time.h>
# #include <string.h>

# #if HAVE_TZNAME
# extern char *tzname[];
# #endif

# int main()
# {
#   /* Note that we need to ensure that not only does tzset(3)
#      do 'something' with localtime, but it works as documented
#      in the library reference and as expected by the test suite.
#      This includes making sure that tzname is set properly if
#      tm->tm_zone does not exist since it is the alternative way
#      of getting timezone info.

#      Red Hat 6.2 doesn't understand the southern hemisphere
#      after New Year's Day.
#   */

#   time_t groundhogday = 1044144000; /* GMT-based */
#   time_t midyear = groundhogday + (365 * 24 * 3600 / 2);

#   putenv(\"TZ=UTC+0\");
#   tzset();
#   if (localtime(&groundhogday)->tm_hour != 0)
#       exit(1);
# #if HAVE_TZNAME
#   /* For UTC, tzname[1] is sometimes \"\", sometimes \"   \" */
#   if (strcmp(tzname[0], \"UTC\") ||
#     (tzname[1][0] != 0 && tzname[1][0] != ' '))
#       exit(1);
# #endif

#   putenv(\"TZ=EST+5EDT,M4.1.0,M10.5.0\");
#   tzset();
#   if (localtime(&groundhogday)->tm_hour != 19)
#       exit(1);
# #if HAVE_TZNAME
#   if (strcmp(tzname[0], \"EST\") || strcmp(tzname[1], \"EDT\"))
#       exit(1);
# #endif

#   putenv(\"TZ=AEST-10AEDT-11,M10.5.0,M3.5.0\");
#   tzset();
#   if (localtime(&groundhogday)->tm_hour != 11)
#       exit(1);
# #if HAVE_TZNAME
#   if (strcmp(tzname[0], \"AEST\") || strcmp(tzname[1], \"AEDT\"))
#       exit(1);
# #endif

# #if HAVE_STRUCT_TM_TM_ZONE
#   if (strcmp(localtime(&groundhogday)->tm_zone, \"AEDT\"))
#       exit(1);
#   if (strcmp(localtime(&midyear)->tm_zone, \"AEST\"))
#       exit(1);
# #endif

#   exit(0);
# }
# ")
# cmake_push_check_state()
# add_cond(CMAKE_REQUIRED_DEFINITIONS HAVE_TZNAME "-DHAVE_TZNAME")
# add_cond(CMAKE_REQUIRED_DEFINITIONS HAVE_STRUCT_TM_TM_ZONE "-DHAVE_STRUCT_TM_TM_ZONE")
# python_platform_test_run(
#   HAVE_WORKING_TZSET
#   "Checking for working tzset()"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()

# # Check for broken unsetenv
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_broken_unsetenv.c)
# file(WRITE ${check_src} "#include <stdlib.h>
# int main() {
#   int res = unsetenv(\"DUMMY\");
# }
# ")
# python_platform_test(
#   HAVE_BROKEN_UNSETENV
#   "Checking for broken unsetenv"
#   ${check_src}
#   INVERT
#   )

# # Define if the system reports an invalid PIPE_BUF value.
# set(HAVE_BROKEN_PIPE_BUF 0)
# if(CMAKE_SYSTEM MATCHES AIX)
#   set(HAVE_BROKEN_PIPE_BUF 1)
# endif()


if(IS_PY3)
# Define if aligned memory access is required
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/aligned_required.c)
file(WRITE ${check_src} "int main()
{
    char s[16];
    int i, *p1, *p2;
    for (i=0; i < 16; i++)
        s[i] = i;
    p1 = (int*)(s+1);
    p2 = (int*)(s+2);
    if (*p1 == *p2)
        return 1;
    return 0;
}
")
python_platform_test_run(
  HAVE_ALIGNED_REQUIRED
  "Checking aligned memory access is required"
  ${check_src}
  INVERT
  )

# Define if mbstowcs(NULL, "text", 0) does not return the number of wide
# chars that would be converted.
set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_broken_mbstowcs.c)
file(WRITE ${check_src} "#include <stdio.h>
#include<stdlib.h>
int main() {
    size_t len = -1;
    const char *str = \"text\";
    len = mbstowcs(NULL, str, 0);
    return (len != 4);
}
")
python_platform_test_run(
  HAVE_BROKEN_MBSTOWCS
  "Checking for broken mbstowcs"
  ${check_src}
  INVERT
  )

# Issue #25658: POSIX hasn't defined that pthread_key_t is compatible with int.
# This checking will be unnecessary after removing deprecated TLS API.
# The cast to long int works around a bug in the HP C Compiler
# version HP92453-01 B.11.11.23709.GP, which incorrectly rejects
# declarations like `int a3[[(sizeof (unsigned char)) >= 0]];'.
# This bug is HP SR number 8606223364.
if(HAVE_PTHREAD_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "pthread.h")
  check_type_size(pthread_key_t SIZEOF_PTHREAD_KEY_T)
  unset(CMAKE_EXTRA_INCLUDE_FILES)

  if("${SIZEOF_PTHREAD_KEY_T}" STREQUAL "${SIZEOF_INT}")
    check_c_source_compiles("#include <pthread.h>\n int main(void){pthread_key_t k; k * 1;}" PTHREAD_KEY_T_IS_COMPATIBLE_WITH_INT)
  endif()
endif()

endif(IS_PY3)

# FIXME: check goto support
# # Check whether the compiler supports computed gotos
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_computed_gotos.c)
# file(WRITE ${check_src} "int main(int argc, char **argv)
#   {
#       static void *targets[1] = { &&LABEL1 };
#       goto LABEL2;
#   LABEL1:
#       return 0;
#   LABEL2:
#       goto *targets[0];
#       return 1;
#   }
# ")
# python_platform_test_run(
#   HAVE_COMPUTED_GOTOS
#   "Checking whether ${CMAKE_C_COMPILER_ID} supports computed gotos"
#   ${check_src}
#   DIRECT
#   )

if(IS_PY3)

# FIXME: compiler support O2 flag
# # Availability of -O2
# cmake_push_check_state()
# list(APPEND CMAKE_REQUIRED_DEFINITIONS -O2)
# check_c_source_compiles("int main () {return 0;}" have_O2)
# cmake_pop_check_state()

# FIXME: _FORTIFY_SOURCE wrappers for memmove and bcopy are incorrect
# # _FORTIFY_SOURCE wrappers for memmove and bcopy are incorrect:
# # http://sourceware.org/ml/libc-alpha/2010-12/msg00009.html
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_glibc_memmove_bug.c)
# file(WRITE ${check_src} "#include <stdio.h>
# #include <stdlib.h>
# #include <string.h>
# void foo(void *p, void *q) { memmove(p, q, 19); }
# int main() {
#   char a[32] = \"123456789000000000\";
#   foo(&a[9], a);
#   if (strcmp(a, \"123456789123456789000000000\") != 0)
#     return 1;
#   foo(a, &a[9]);
#   if (strcmp(a, \"123456789000000000\") != 0)
#     return 1;
#   return 0;
# }
# ")
# cmake_push_check_state()
# add_cond(CMAKE_REQUIRED_DEFINITIONS have_O2 "-O2;-D_FORTIFY_SOURCE=2")
# python_platform_test_run(
#   HAVE_GLIBC_MEMMOVE_BUG
#   "Checking for glibc _FORTIFY_SOURCE/memmove bug"
#   ${check_src}
#   INVERT
#   )
# cmake_pop_check_state()

# FIXME: test IPA_PURE_CONST_BUG
# # HAVE_IPA_PURE_CONST_BUG
# if(HAVE_GCC_ASM_FOR_X87 AND CMAKE_C_COMPILER_ID MATCHES "GNU")
#   # Some versions of gcc miscompile inline asm:
#   # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=46491
#   # http://gcc.gnu.org/ml/gcc/2010-11/msg00366.html
#   set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_ipa_pure_const_bug.c)
#   file(WRITE ${check_src} "__attribute__((noinline)) int
#   foo(int *p) {
#     int r;
#     asm ( \"movl \\$6, (%1)\\n\\t\"
#           \"xorl %0, %0\\n\\t\"
#           : \"=r\" (r) : \"r\" (p) : \"memory\"
#     );
#     return r;
#   }
#   int main() {
#     int p = 8;
#     if ((foo(&p) ? : p) != 6)
#       return 1;
#     return 0;
#   }
#   ")
#   python_platform_test_run(
#     HAVE_IPA_PURE_CONST_BUG
#     "Checking for gcc ipa-pure-const bug"
#     ${check_src}
#     INVERT
#     )
# endif()

# FIXME: check stdatomic.h
# # Check for stdatomic.h
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_stdatomic_h.c)
# file(WRITE ${check_src} "#include <stdatomic.h>
# atomic_int value = ATOMIC_VAR_INIT(1);
# _Atomic void *py_atomic_address = (void*) &value;
# int main() {
#   int loaded_value = atomic_load(&value);
#   return 0;
# }
# ")
# python_platform_test(
#   HAVE_STD_ATOMIC
#   "Checking for stdatomic.h"
#   ${check_src}
#   DIRECT
#   )

# FIXME: check builtin atomics
# # Has builtin atomics
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_builtin_atomic.c)
# file(WRITE ${check_src} "volatile int val = 1;
# int main() {
#   __atomic_load_n(&val, __ATOMIC_SEQ_CST);
#   return 0;
# }
# ")
# python_platform_test(
#   HAVE_BUILTIN_ATOMIC
#   "Checking for GCC >= 4.7 __atomic builtins"
#   ${check_src}
#   DIRECT
#   )

endif(IS_PY3)

# FIXME: check long long type
# if(HAVE_LONG_LONG)
#   # Checking for %lld and %llu printf() format support
#   set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_have_long_long_format.c)
#   file(WRITE ${check_src} "#include <stdio.h>
# #include <stddef.h>
# #include <string.h>

# #ifdef HAVE_SYS_TYPES_H
# #include <sys/types.h>
# #endif

# int main()
# {
#     char buffer[256];

#     if (sprintf(buffer, \"%lld\", (long long)123) < 0)
#         return 1;
#     if (strcmp(buffer, \"123\"))
#         return 1;

#     if (sprintf(buffer, \"%lld\", (long long)-123) < 0)
#         return 1;
#     if (strcmp(buffer, \"-123\"))
#         return 1;

#     if (sprintf(buffer, \"%llu\", (unsigned long long)123) < 0)
#         return 1;
#     if (strcmp(buffer, \"123\"))
#         return 1;

#     return 0;
# }
# ")
#   cmake_push_check_state()
#   add_cond(CMAKE_REQUIRED_DEFINITIONS HAVE_SYS_TYPES_H "-DHAVE_SYS_TYPES_H")
#   python_platform_test_run(
#     HAVE_LONG_LONG_FORMAT
#     "Checking for %lld and %llu printf() format support"
#     ${check_src}
#     DIRECT
#     )
#   cmake_pop_check_state()
#   if(HAVE_LONG_LONG_FORMAT)
#     set(PY_FORMAT_LONG_LONG "ll")
#   endif()
# endif()


# FIXME: check %zd format support
# # Checking for %zd printf() format support
# set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/ac_cv_have_size_t_format.c)
# file(WRITE ${check_src} "#include <stdio.h>
# #include <stddef.h>
# #include <string.h>

# #ifdef HAVE_SYS_TYPES_H
# #include <sys/types.h>
# #endif

# #ifdef HAVE_SSIZE_T
# typedef ssize_t Py_ssize_t;
# #elif SIZEOF_VOID_P == SIZEOF_LONG
# typedef long Py_ssize_t;
# #else
# typedef int Py_ssize_t;
# #endif

# int main()
# {
#     char buffer[256];
#     if(sprintf(buffer, \"%zd\", (size_t)123) < 0)
#         return 1;
#     if (strcmp(buffer, \"123\"))
#         return 1;
#     if (sprintf(buffer, \"%zd\", (Py_ssize_t)-123) < 0)
#         return 1;
#     if (strcmp(buffer, \"-123\"))
#         return 1;
#     return 0;
# }
# ")
# cmake_push_check_state()
# add_cond(CMAKE_REQUIRED_DEFINITIONS HAVE_SYS_TYPES_H "-DHAVE_SYS_TYPES_H")
# add_cond(CMAKE_REQUIRED_DEFINITIONS HAVE_SSIZE_T "-DHAVE_SSIZE_T")
# add_cond(CMAKE_REQUIRED_DEFINITIONS SIZEOF_VOID_P "-DSIZEOF_VOID_P=${SIZEOF_VOID_P}")
# add_cond(CMAKE_REQUIRED_DEFINITIONS SIZEOF_LONG "-DSIZEOF_LONG=${SIZEOF_LONG}")
# python_platform_test_run(
#   HAVE_SIZE_T_FORMAT
#   "Checking for %zd printf() format support()"
#   ${check_src}
#   DIRECT
#   )
# cmake_pop_check_state()
# if(HAVE_SIZE_T_FORMAT)
#   set(PY_FORMAT_SIZE_T "z")
# endif()


##########################################################
# FIXME: support zlib
# if(ZLIB_LIBRARIES)
#   cmake_push_check_state()
#   set(CFG_HEADERS_SAVE ${CFG_HEADERS})

#   set(CFG_HEADERS ${CFG_HEADERS} zlib.h)
#   add_cond(CMAKE_REQUIRED_INCLUDES ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIRS})
#   add_cond(CMAKE_REQUIRED_LIBRARIES ZLIB_LIBRARIES ${ZLIB_LIBRARIES})
#   check_symbol_exists(inflateCopy      "${CFG_HEADERS}" HAVE_ZLIB_COPY)

#   set(CFG_HEADERS ${CFG_HEADERS_SAVE})
#   cmake_pop_check_state()
# endif()

# Check if tirpc static library and its dependencies can be linked statically
# and set TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED and TIRPC_STATIC_LIBRARIES.
#
# If the tirpc static library is built with the gssapi feature, and no gssapi_krb5
# static library can be found, the python extensions depending on tirpc should be
# disabled
#
# Indeed, minimal support for static linking in krb5 was only re-introduced starting
# with krb5-1.19.2 (see [1]) which may not yet be available (see [2]).
#
# [1] https://github.com/krb5/krb5/commit/af44e8dee9b5bfffb05eb5a9b89c79c02ab7c2ff
# [2] https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=439039

# set(_tirpc_library_static_dependencies_required 0)
# set(_tirpc_static_libraries)

# if(WITH_STATIC_DEPENDENCIES AND TIRPC_LIBRARY)

#   # Attempt to find gssapi_krb5 static library
#   set(__CURRENT_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
#   set(CMAKE_FIND_LIBRARY_SUFFIXES "${CMAKE_STATIC_LIBRARY_SUFFIX}")
#   find_library(GSSAPI_KRB5_STATIC_LIBRARY gssapi_krb5)
#   set(CMAKE_FIND_LIBRARY_SUFFIXES ${__CURRENT_CMAKE_FIND_LIBRARY_SUFFIXES})

#   # Attempt to find gssapi_krb5 shared library
#   set(__CURRENT_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
#   if(NOT ${CMAKE_IMPORT_LIBRARY_SUFFIX} STREQUAL "")
#     set(CMAKE_FIND_LIBRARY_SUFFIXES "${CMAKE_IMPORT_LIBRARY_SUFFIX}")
#   else()
#     set(CMAKE_FIND_LIBRARY_SUFFIXES "${CMAKE_SHARED_LIBRARY_SUFFIX}")
#   endif()
#   find_library(GSSAPI_KRB5_SHARED_LIBRARY gssapi_krb5)
#   set(CMAKE_FIND_LIBRARY_SUFFIXES ${__CURRENT_CMAKE_FIND_LIBRARY_SUFFIXES})

#   if(GSSAPI_KRB5_SHARED_LIBRARY)
#     # Attempt to test if tirpc library is built with the krb5 gssapi feature
#     # by looking for the symbol "authgss_create" linking against the corresponding
#     # shared library.

#     # Save variables
#     set(__CURRENT_CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS})
#     cmake_push_check_state()

#     list(APPEND CMAKE_REQUIRED_LIBRARIES ${TIRPC_LIBRARY})
#     add_cond(CMAKE_REQUIRED_LIBRARIES  CMAKE_USE_PTHREADS_INIT  "${CMAKE_THREAD_LIBS_INIT}")
#     add_cond(CMAKE_REQUIRED_LIBRARIES  GSSAPI_KRB5_SHARED_LIBRARY  "${GSSAPI_KRB5_SHARED_LIBRARY}")

#     # If any, remove "-static" flags
#     string(REPLACE "-static" "" _cmake_exe_linker_flags "${CMAKE_EXE_LINKER_FLAGS}")
#     set(CMAKE_EXE_LINKER_FLAGS ${_cmake_exe_linker_flags} CACHE STRING "" FORCE)

#     check_symbol_exists("authgss_create" "tirpc/rpc/auth_gss.h" TIRPC_WITH_AUTHGSS_CREATE)

#     # Restore variables
#     cmake_pop_check_state()
#     set(CMAKE_EXE_LINKER_FLAGS ${__CURRENT_CMAKE_EXE_LINKER_FLAGS} CACHE STRING "Flags used by the linker during all build types." FORCE)

#     if(TIRPC_WITH_AUTHGSS_CREATE)
#       set(_tirpc_library_static_dependencies_required 1)
#       set(_tirpc_static_libraries ${GSSAPI_KRB5_STATIC_LIBRARY})
#     endif()
#   else()
#     # If shared library can't be found, we assume the static library is required.
#     set(_tirpc_library_static_dependencies_required 1)
#     set(_tirpc_static_libraries ${GSSAPI_KRB5_STATIC_LIBRARY})
#   endif()

# endif()

# if(NOT DEFINED TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED)
#   set(TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED ${_tirpc_library_static_dependencies_required} CACHE BOOL "")
#   message(STATUS "Setting TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED to ${TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED}")
# endif()
# if(TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED AND NOT DEFINED TIRPC_STATIC_LIBRARIES)
#   set(TIRPC_STATIC_LIBRARIES ${_tirpc_static_libraries} CACHE BOOL "")
#   message(STATUS "Setting TIRPC_STATIC_LIBRARIES to ${TIRPC_STATIC_LIBRARIES}")
# endif()

############################################
# FIXME: check CAN support
# if(IS_PY3)
# # Check for CAN_RAW_FD_FRAMES
# check_c_source_compiles("#include <linux/can/raw.h>\n int main () { int can_raw_fd_frames = CAN_RAW_FD_FRAMES; }" HAVE_LINUX_CAN_RAW_FD_FRAMES)

# # Check for CAN_RAW_JOIN_FILTERS
# check_c_source_compiles("#include <linux/can/raw.h>\n int main () { int can_raw_join_filters = CAN_RAW_JOIN_FILTERS; }" HAVE_LINUX_CAN_RAW_JOIN_FILTERS)
# endif()

# set(HAVE_OSX105_SDK 0)
# if(APPLE)
#   # Check for OSX 10.5 SDK or later
#   set(check_src ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/have_osx105_sdk.c)
#   file(WRITE ${check_src} "#include <Carbon/Carbon.h>
# int main(int argc, char* argv[]){FSIORefNum fRef = 0; return 0;}")
#   python_platform_test(
#     HAVE_OSX105_SDK
#     "Checking for OSX 10.5 SDK or later"
#     ${check_src}
#     DIRECT
#     )
# endif()

# todo
set(PTHREAD_SYSTEM_SCHED_SUPPORTED 1)
set(HAVE_DEVICE_MACROS ${HAVE_MAKEDEV})

# endif()

# setup the python platform
set(PY_PLATFORM generic)

if(CMAKE_SYSTEM MATCHES Linux)
  set(PY_PLATFORM linux)
endif()

if(CMAKE_SYSTEM MATCHES Darwin)
  set(PY_PLATFORM darwin)
endif()

if(CMAKE_SYSTEM MATCHES FreeBSD)
  set(PY_PLATFORM freebsd5)  # which version to use ?
endif()

if(CMAKE_SYSTEM MATCHES NetBSD)
  set(PY_PLATFORM netbsd1)
endif()

if(CMAKE_SYSTEM MATCHES AIX)
  set(PY_PLATFORM aix4)
endif()

if(CMAKE_SYSTEM MATCHES BeOS)
  set(PY_PLATFORM beos5)
endif()

if(CMAKE_SYSTEM MATCHES IRIX)
  set(PY_PLATFORM irix6)
endif()

if(CMAKE_SYSTEM MATCHES SunOS)
  set(PY_PLATFORM sunos5)
endif()

if(CMAKE_SYSTEM MATCHES UnixWare)
  set(PY_PLATFORM unixware7)
endif()

if(CMAKE_SYSTEM MATCHES Windows)
  set(PY_PLATFORM win32)
endif()

option(WITH_DOC_STRINGS "Enable documentation strings" ON)
