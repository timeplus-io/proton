include (CheckIncludeFiles)
include (CheckTypeSize)
include (CheckSymbolExists)
include (TestBigEndian)

set (FFI_SOURCE_DIR "${proton_SOURCE_DIR}/contrib/libffi")
set (FFI_CMAKE_DIR "${proton_SOURCE_DIR}/contrib/libffi-cmake")

# config variables for ffi.h.in
set (VERSION 3.4.6)

set (KNOWN_PROCESSORS x86 x86_64 amd64 arm arm64 i386 i686 armv7l armv7-a aarch64)

string (TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" lower_system_processor)

if (NOT lower_system_processor IN_LIST KNOWN_PROCESSORS)
    message (FATAL_ERROR "Unknown processor: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

if (lower_system_processor MATCHES "arm64|aarch64")
    set (TARGET ARM64)
elseif (lower_system_processor MATCHES "arm")
    set (TARGET ARM)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin"  AND CMAKE_SIZEOF_VOID_P EQUAL 4)
    set (TARGET X86_DARWIN)
elseif (CMAKE_SIZEOF_VOID_P EQUAL 8)
    set (TARGET X86_64)
elseif (CMAKE_SIZEOF_VOID_P EQUAL 4)
    set (TARGET X86)
else ()
    message (FATAL_ERROR "Cannot determine target. Please consult ${CMAKE_CURRENT_SOURCE_DIR}/configure.ac and add your platform to this CMake file.")
endif ()

set (FFI_EXEC_TRAMPOLINE_TABLE 0)

if (OS_LINUX)
    set (EH_FRAME_FLAGS "a")
elseif (OS_DARWIN)
    set (EH_FRAME_FLAGS "aw")
else ()
    message (FATAL_ERROR "libffi: Unsupported OS")
endif ()

set (HAVE_AS_ASCII_PSEUDO_OP 1)
set (HAVE_AS_STRING_PSEUDO_OP 1)

if (ARCH_AMD64)
    set (HAVE_AS_X86_PCREL 1)
endif ()

if (OS_LINUX)
    set (HAVE_HIDDEN_VISIBILITY_ATTRIBUTE 1)
endif()

check_include_files (alloca.h HAVE_ALLOCA_H)
check_include_files (memory.h HAVE_MEMORY_H)
check_include_files (stdint.h HAVE_STDINT_H)
check_include_files (stdio.h HAVE_STDIO_H)
check_include_files (stdlib.h HAVE_STDLIB_H)
check_include_files (strings.h HAVE_STRINGS_H)
check_include_files (string.h HAVE_STRING_H)
check_include_files (sys/memfd.h HAVE_SYS_MEMFD_H)
check_include_files (sys/stat.h HAVE_SYS_STAT_H)
check_include_files (sys/types.h HAVE_SYS_TYPES_H)
check_include_files (unistd.h HAVE_UNISTD_H)
check_include_files (sys/mman.h HAVE_SYS_MMAN_H)
check_include_files ("stdlib.h;stdarg.h;string.h;float.h" STDC_HEADERS)

check_type_size (double SIZEOF_DOUBLE)
check_type_size ("long double" SIZEOF_LONG_DOUBLE)
set (HAVE_LONG_DOUBLE ${SIZEOF_LONG_DOUBLE})
set (HAVE_LONG_DOUBLE_VARIANT 0)
check_type_size (size_t SIZEOF_SIZE_T)

set (SYMBOL_UNDERSCORE 0)

macro (add_cond var cond item)
  if (${cond})
    set (${var} ${${var}} ${item})
  endif ()
endmacro ()

set (CHECK_HEADERS )
add_cond (CHECK_HEADERS HAVE_STRING_H string.h)
add_cond (CHECK_HEADERS HAVE_SYS_MMAN_H sys/mman.h)

check_symbol_exists (memcpy CHECK_HEADERS HAVE_MEMCPY)
check_symbol_exists (memfd_create CHECK_HEADERS HAVE_MEMFD_CREATE)

TEST_BIG_ENDIAN (WORDS_BIGENDIAN)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set (FFI_DEBUG 1)
endif()

# mimic layout of original buildsystem
configure_file (${FFI_SOURCE_DIR}/include/ffi.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/ffi.h)

configure_file (${FFI_CMAKE_DIR}/fficonfig.h.in ${CMAKE_CURRENT_BINARY_DIR}/fficonfig.h)

if ("${TARGET}" STREQUAL "ARM64")
    file (COPY ${FFI_SOURCE_DIR}/src/aarch64/ffitarget.h DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/include)
elseif ("${TARGET}" STREQUAL "ARM")
    file (COPY ${FFI_SOURCE_DIR}/src/arm/ffitarget.h DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/include)
else ()
    file (COPY ${FFI_SOURCE_DIR}/src/x86/ffitarget.h DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/include)
endif ()

set (FFI_INCLUDE_DIRS
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${FFI_SOURCE_DIR}/include
    ${FFI_SOURCE_DIR})

set (FFI_SOURCES
    ${FFI_SOURCE_DIR}/src/closures.c
    ${FFI_SOURCE_DIR}/src/prep_cif.c
    ${FFI_SOURCE_DIR}/src/types.c
    ${FFI_SOURCE_DIR}/src/tramp.c)

if ("${TARGET}" STREQUAL "ARM64")
    set (FFI_SOURCES
        ${FFI_SOURCES}
        ${FFI_SOURCE_DIR}/src/aarch64/ffi.c)
elseif ("${TARGET}" STREQUAL "ARM")
    set (FFI_SOURCES
        ${FFI_SOURCES}
        ${FFI_SOURCE_DIR}/src/arm/ffi.c)
else ()
    set (FFI_SOURCES
        ${FFI_SOURCES}
        ${FFI_SOURCE_DIR}/src/java_raw_api.c
        ${FFI_SOURCE_DIR}/src/raw_api.c)
    if ("${TARGET}" STREQUAL "X86_DARWIN" OR "${TARGET}" STREQUAL "X86")
        set (FFI_SOURCES
            ${FFI_SOURCES}
            ${FFI_SOURCE_DIR}/src/x86/ffi.c)
    elseif ("${TARGET}" STREQUAL "X86_64")
        set (FFI_SOURCES
            ${FFI_SOURCES}
            ${FFI_SOURCE_DIR}/src/x86/ffi64.c
            ${FFI_SOURCE_DIR}/src/x86/ffiw64.c)
    endif ()
endif ()

macro (add_assembly ASMFILE)
    get_filename_component (ASMFILE_FULL "${ASMFILE}" ABSOLUTE)
    list (APPEND FFI_SOURCES ${ASMFILE})
endmacro ()

if ("${TARGET}" STREQUAL "X86")
    set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -m32")
endif ()

# FIXME: remove target windows
if ("${TARGET}" STREQUAL "X86" OR "${TARGET}" STREQUAL "X86_DARWIN")
    add_assembly (${FFI_SOURCE_DIR}/src/x86/sysv.S)
elseif ("${TARGET}" STREQUAL "X86_64")
    add_assembly (${FFI_SOURCE_DIR}/src/x86/unix64.S)
    add_assembly (${FFI_SOURCE_DIR}/src/x86/win64.S)
elseif ("${TARGET}" STREQUAL "ARM")
    add_assembly (${FFI_SOURCE_DIR}/src/arm/sysv.S)
elseif ("${TARGET}" STREQUAL "ARM64")
    add_assembly (${FFI_SOURCE_DIR}/src/aarch64/sysv.S)
else ()
    message (FATAL_ERROR "Target not implemented")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    list (APPEND FFI_SOURCES ${FFI_SOURCE_DIR}/src/debug.c)
endif ()

add_library (libffi ${FFI_SOURCES})
target_include_directories (libffi PUBLIC ${FFI_INCLUDE_DIRS})
target_compile_definitions (libffi PUBLIC FFI_BUILDING)

add_library (ch_contrib::libffi ALIAS libffi)
