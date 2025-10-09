#[[
    This module enables Ccache support by inserting a ccache executable as
    the compiler launcher for C and C++ if there is a ccache executable availbale
    on the system.

    Ccache support will be automatically enabled if it is found on the system.
    Ccache can be forced on or off by setting the MONGO_USE_CCACHE CMake option to
    ON or OFF.
]]

# Find and enable ccache for compiling
find_program (CCACHE_EXECUTABLE ccache)

if (CCACHE_EXECUTABLE AND NOT DEFINED MONGO_USE_CCACHE)
    message (STATUS "Found Ccache: ${CCACHE_EXECUTABLE}")
    execute_process(
        COMMAND "${CCACHE_EXECUTABLE}" --version
        OUTPUT_VARIABLE _out
        OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    set (_enable TRUE)
    # Avoid spurious "ccache.conf: No such file or directory" errors due to
    # ccache being invoked in parallel, which was patched in ccache version 3.4.3.
    if (_out MATCHES "^ccache version ([0-9]+\\.[0-9]+\\.[0-9]+)")
        set (_version "${CMAKE_MATCH_1}")
        message (STATUS "Detected Ccache version: ${_version}")
        if (_version VERSION_LESS "3.4.3")
            message (STATUS "Not using Ccache: Detected Ccache version ${_version} "
                            "is less than 3.4.3, which may lead to spurious failures "
                            "when run in parallel. See https://github.com/ccache/ccache/issues/260 "
                            "for more information.")
            set (_enable FALSE)
        endif ()
    else ()
        message (STATUS "Note: Unable to automatically detect Ccache from from output: [[${_out}]]")
    endif ()
    option (MONGO_USE_CCACHE "Use Ccache when compiling" "${_enable}")
endif ()

if (MONGO_USE_CCACHE)
    message (STATUS "Compiling with Ccache enabled. Disable by setting MONGO_USE_CCACHE to OFF")
    set (CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    set (CMAKE_C_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
endif ()
