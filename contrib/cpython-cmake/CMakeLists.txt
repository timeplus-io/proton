if (NOT ENABLE_PYTHON_UDF)
    message(STATUS "Not using python udf (skip cpython)")
    return()
endif()

set(CPYTHON_SOURCE_DIR "${proton_SOURCE_DIR}/contrib/cpython")
set(CPYTHON_HEADER_DIR "${CPYTHON_SOURCE_DIR}/Include")
set(CPYTHON_CMAKE_SOURCE_DIR "${proton_SOURCE_DIR}/contrib/cpython-cmake")
set(CPYTHON_CMAKE_BINARY_DIR "${proton_BINARY_DIR}/contrib/cpython-cmake")

set(PYTHON_VERSION "3.10.14" CACHE STRING "The version of Python to build.")
string(REPLACE "." ";" VERSION_LIST ${PYTHON_VERSION})
list(GET VERSION_LIST 0 PY_VERSION_MAJOR)
list(GET VERSION_LIST 1 PY_VERSION_MINOR)
list(GET VERSION_LIST 2 PY_VERSION_PATCH)
set(PY_VERSION "${PY_VERSION_MAJOR}.${PY_VERSION_MINOR}.${PY_VERSION_PATCH}")

if(NOT EXISTS "${CPYTHON_SOURCE_DIR}/pyconfig.h.in")
    message(FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init --recursive")
endif()

# Include helper functions，macro: add_python_extension，show_extension_summary
include(cmake/Extensions.cmake)
include(CMakeDependentOption)
include(CMakeParseArguments)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(Py_DEBUG ON)
endif()

# Extract version string from python source (Copied from FindPythonLibs.cmake)
file(STRINGS "${CPYTHON_SOURCE_DIR}/Include/patchlevel.h" python_version_str
    REGEX "^#define[ \t]+PY_VERSION[ \t]+\"[^\"]+\"")
string(REGEX REPLACE "^#define[ \t]+PY_VERSION[ \t]+\"([^\"]+)\".*" "\\1"
      PY_VERSION_LONG "${python_version_str}")

# Extract version info and set variables PY_VERSION, PY_VERSION_(MAJOR|MINOR|PATCH), PY_RELEASE_(LEVEL|SERIAL)
include(cmake/PythonExtractVersionInfo.cmake)
python_extract_version_info(
  VERSION_STRING "${PY_VERSION_LONG}"
  )
message(STATUS "PY_VERSION     : ${PY_VERSION}")
message(STATUS "PY_VERSION_LONG: ${PY_VERSION_LONG}")

# Convenience boolean variables to easily test python version
set(IS_PY3 0)
set(IS_PY2 1)
if(PY_VERSION_MAJOR VERSION_GREATER 2)
    set(IS_PY3 1)
    set(IS_PY2 0)
endif()

if(PY_VERSION VERSION_LESS "3.7")
    option(WITH_THREAD "Compile in rudimentary thread support" ON)
else()
    if(DEFINED WITH_THREAD)
        message(AUTHOR_WARNING "WITH_THREAD option is *NOT* supported with Python < 3.7. Current version is ${PY_VERSION}")
    endif()
endif()

include(cmake/ConfigureChecks.cmake)

configure_file(cmake/config-unix/pyconfig.h.in
               ${CPYTHON_CMAKE_BINARY_DIR}/include/pyconfig.h)

add_subdirectory(cmake/extensions)
add_subdirectory(cmake/libpython)
add_subdirectory(cmake/python)

add_library(ch_contrib::python ALIAS libpython-static)
