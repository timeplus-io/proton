#[[
    This module conditionally enables -fuse-ld=lld if it is supported by the compiler.
    This is purely for build performance, and has no apparent effect on the generated
    code.

    LLD is *significantly* faster to link and produces significantly better link-time
    error diagnostics.

    LLD linking will be automatically enabled if it is supported, but can be forced
    on or off by setting the MONGO_USE_LLD CMake option to ON or OFF.
]]

if (NOT COMMAND add_link_options)
    # This only works on new-enough versions of CMake that support LINK_OPTIONS as
    # a separate configuration entity
    return ()
endif ()

include (CMakePushCheckState)
include (CheckCSourceCompiles)
cmake_push_check_state (RESET)
    # Newer GNU compilers support lld with the '-fuse-ld=lld' flag.
    set (CMAKE_REQUIRED_FLAGS "-fuse-ld=lld")
    set (CMAKE_REQUIRED_LINK_OPTIONS "-fuse-ld=lld")
    check_c_source_compiles ([[
        #include <stdio.h>

        int main (void) {
            puts ("Hello, world!");
            return 0;
        }
    ]] HAVE_LLD_LINKER_SUPPORT)
cmake_pop_check_state ()


if (HAVE_LLD_LINKER_SUPPORT)
    # Expose an option to toggle usage of lld
    option (MONGO_USE_LLD "Link runtime binaries using LLVM's lld linker" ON)
elseif (NOT DEFINED _MONGO_LD_LLD_LINKER)
    # We don't have -fuse-lld support, but that might be because of a misconfig in
    # the environment. Issue a *one-time* diagnostic telling the user if they *almost*
    # have LLD support. This branch is only for diagnostic purposes.
    find_program (_MONGO_LD_LLD_LINKER
        NAMES ld.lld ld.lld-13 ld.lld-12 ld.lld-11 ld.lld-10 ld.lld-9 ld.lld-8 ld.lld-7 ld.lld-6 ld.lld-5
        )
    mark_as_advanced (_MONGO_LD_LLD_LINKER)
    # If we found one, we are compiling with GCC, *and* the found lld has a version suffix, issue a message
    # telling the user how they might be able to get lld work.
    if (_MONGO_LD_LLD_LINKER AND CMAKE_C_COMPILER_ID STREQUAL "GNU" AND NOT _MONGO_LD_LLD_LINKER MATCHES "ld.lld$")
        message (STATUS "NOTE: A GNU-compatible lld linker was found (${_MONGO_LD_LLD_LINKER}), but support from GCC requires that")
        message (STATUS "  the 'ld.lld' binary be named *exactly* 'ld.lld' (without a version suffix!)")
        message (STATUS "  To enable 'lld' support, try creating a symlink of 'ld.lld' somewhere on your PATH that points")
        message (STATUS "  to '${_MONGO_LD_LLD_LINKER}'")
    endif ()
endif ()

if (MONGO_USE_LLD)
    message (STATUS "Linking using LLVM lld. Disable by setting MONGO_USE_LLD to OFF")
    add_link_options (-fuse-ld=lld)
endif ()
