if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(USE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

# Utils used in package
add_subdirectory (report)

# Not used in package
if (ENABLE_UTILS)
    add_subdirectory (compressor)
    add_subdirectory (corrector_utf8)
    add_subdirectory (wikistat-loader)
    add_subdirectory (check-marks)
    add_subdirectory (checksum-for-compressed-block)
    add_subdirectory (wal-dump)

    # memcpy_jart.S contains position dependent code
    if (NOT CMAKE_POSITION_INDEPENDENT_CODE AND OS_LINUX AND ARCH_AMD64)
        add_subdirectory (memcpy-bench)
    endif ()
endif ()

add_subdirectory (package)
