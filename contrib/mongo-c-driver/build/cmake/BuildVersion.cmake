include_guard(GLOBAL)

include(MongoSettings)

# We use Python to calculate the BUILD_VERSION value
find_package(Python COMPONENTS Interpreter)

set(_CALC_VERSION_PY "${CMAKE_CURRENT_LIST_DIR}/../calc_release_version.py")

#[[
    Attempts to find the current build version string. If VERSION_CURRENT exists
    in the current source directory, uses that. Otherwise, runs calc_release_version.py
    to compute the version from the Git history.

    The computed build version is set in the parent scope according to `outvar`.
]]
function(compute_build_version outvar)
    list(APPEND CMAKE_MESSAGE_CONTEXT ${CMAKE_CURRENT_FUNCTION})
    # If it is present, defer to the VERSION_CURRENT file:
    set(ver_cur_file "${CMAKE_CURRENT_SOURCE_DIR}/VERSION_CURRENT")
    if(EXISTS "${ver_cur_file}")
        message(DEBUG "Using existing VERSION_CURRENT file as BUILD_VERSION [${ver_cur_file}]")
        file(READ "${ver_cur_file}" version)
        set("${outvar}" "${version}" PARENT_SCOPE)
        return()
    endif()
    # Otherwise, we require Python:
    if(NOT TARGET Python::Interpreter)
        message(WARNING "No default build version could be calculated (Python was not found)")
        set("${outvar}" "0.0.0-unknown+no-python-found")
        return()
    endif()
    get_target_property(py Python::Interpreter IMPORTED_LOCATION)
    message(STATUS "Computing the current release version...")
    execute_process(
        COMMAND "${py}" "${_CALC_VERSION_PY}"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE output
        RESULT_VARIABLE retc
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(retc)
        message(FATAL_ERROR "Computing the build version failed! [${retc}]:\n${out}")
    endif()
    message(DEBUG "calc_release_version.py returned output: “${output}”")
    set("${outvar}" "${output}" PARENT_SCOPE)
endfunction()

# Compute the BUILD_VERSION if it is not already defined:
if(NOT DEFINED BUILD_VERSION)
    compute_build_version(BUILD_VERSION)
endif()

# Set a BUILD_VERSION_SIMPLE, which is just a three-number-triple that CMake understands
string (REGEX REPLACE "([0-9]+\\.[0-9]+\\.[0-9]+).*$" "\\1" BUILD_VERSION_SIMPLE "${BUILD_VERSION}")
