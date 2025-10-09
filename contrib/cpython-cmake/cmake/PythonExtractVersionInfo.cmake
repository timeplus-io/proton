
# Given a version string of the form "X.Y.Z[rcN|[a-z]N]", extract
# major, minor, patch version as well as release level (rc, any letter)
# and serial (an integer)
#
# The function will systematically set the following variable in the caller
# scope:
#
#   PY_VERSION_MAJOR  - X
#   PY_VERSION_MINOR  - Y
#   PY_VERSION_PATCH  - Z
#   PY_RELEASE_LEVEL  - rc, a, b, c, ..., z or an empty string
#   PY_RELEASE_SERIAL - an integer or an empty string
#   PY_VERSION        - X.Y.Z
#
function(python_extract_version_info)
    set(options)
    set(oneValueArgs
        VERSION_STRING
        )
    set(multiValueArgs)
    cmake_parse_arguments(MY
        "${options}"
        "${oneValueArgs}"
        "${multiValueArgs}"
        ${ARGN}
        )

    # Sanity checks
    # XXX TBD

    set(version_str ${MY_VERSION_STRING})

    # version_str             : 2.7.14 | 2.7.14rc1 | 3.6.0a4 | 3.7.0b1
    # version_major           : 2      | 2         | 3       | 3
    # version_minor           :   7    |   7       |   6     |   7
    # version_patch           :     14 |     14    |     0   |     0
    # release_level_and_serial:        |       rc1 |      a4 |      b1
    # release_level           :        |       rc  |      a  |      b
    # release_serial          :        |         1 |       4 |       1
    # version                 : 2.7.14 | 2.7.14    | 3.6.0   | 3.7.0

    # Code adpated from FindPythonLibs.cmake
    string(REGEX REPLACE "([0-9]+)\\..+" "\\1"                version_major            ${version_str})
    string(REGEX REPLACE "[0-9]+\\.([0-9]+)\\..+" "\\1"       version_minor            ${version_str})
    string(REGEX REPLACE "[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" version_patch            ${version_str})
    string(REGEX REPLACE "[0-9]+\\.[0-9]+\\.[0-9]+(.*)" "\\1" release_level_and_serial ${version_str})
    string(REGEX REPLACE "([a-z]+)[0-9]+" "\\1"               release_level            "${release_level_and_serial}")
    string(REGEX REPLACE "[a-z]+([0-9]+)" "\\1"               release_serial           "${release_level_and_serial}")

    set(version "${version_major}.${version_minor}.${version_patch}")

    set(PY_VERSION_MAJOR  ${version_major} PARENT_SCOPE)
    set(PY_VERSION_MINOR  ${version_minor} PARENT_SCOPE)
    set(PY_VERSION_PATCH  ${version_patch} PARENT_SCOPE)
    set(PY_RELEASE_LEVEL  ${release_level} PARENT_SCOPE)
    set(PY_RELEASE_SERIAL ${release_serial} PARENT_SCOPE)
    set(PY_VERSION        ${version} PARENT_SCOPE)
endfunction()

#
# cmake -DTEST_python_extract_version_info:BOOL=ON -P PythonExtractVersionInfo.cmake
#
function(python_extract_version_info_test)

    function(display_output_values)
        foreach(varname
                VERSION_MAJOR
                VERSION_MINOR
                VERSION_PATCH
                RELEASE_LEVEL
                RELEASE_SERIAL
                VERSION
            )
            message("PY_${varname}: ${PY_${varname}}")
        endforeach()
    endfunction()

    set(id 1)
    set(case${id}_input_version_long         "2.7.14")
    set(case${id}_expected_PY_VERSION_MAJOR  "2")
    set(case${id}_expected_PY_VERSION_MINOR  "7")
    set(case${id}_expected_PY_VERSION_PATCH  "14")
    set(case${id}_expected_PY_RELEASE_LEVEL  "")
    set(case${id}_expected_PY_RELEASE_SERIAL "")
    set(case${id}_expected_PY_VERSION        "2.7.14")

    set(id 2)
    set(case${id}_input_version_long         "2.7.14rc1")
    set(case${id}_expected_PY_VERSION_MAJOR  "2")
    set(case${id}_expected_PY_VERSION_MINOR  "7")
    set(case${id}_expected_PY_VERSION_PATCH  "14")
    set(case${id}_expected_PY_RELEASE_LEVEL  "rc")
    set(case${id}_expected_PY_RELEASE_SERIAL "1")
    set(case${id}_expected_PY_VERSION        "2.7.14")

    set(id 3)
    set(case${id}_input_version_long         "3.6.0a4")
    set(case${id}_expected_PY_VERSION_MAJOR  "3")
    set(case${id}_expected_PY_VERSION_MINOR  "6")
    set(case${id}_expected_PY_VERSION_PATCH  "0")
    set(case${id}_expected_PY_RELEASE_LEVEL  "a")
    set(case${id}_expected_PY_RELEASE_SERIAL "4")
    set(case${id}_expected_PY_VERSION        "3.6.0")

    set(id 4)
    set(case${id}_input_version_long         "10.744.42b66")
    set(case${id}_expected_PY_VERSION_MAJOR  "10")
    set(case${id}_expected_PY_VERSION_MINOR  "744")
    set(case${id}_expected_PY_VERSION_PATCH  "42")
    set(case${id}_expected_PY_RELEASE_LEVEL  "b")
    set(case${id}_expected_PY_RELEASE_SERIAL "66")
    set(case${id}_expected_PY_VERSION        "10.744.42")

    foreach(caseid RANGE 1 ${id})
        set(input "${case${caseid}_input_version_long}")
        python_extract_version_info(VERSION_STRING "${input}")
        foreach(varname
                VERSION_MAJOR
                VERSION_MINOR
                VERSION_PATCH
                RELEASE_LEVEL
                RELEASE_SERIAL
                VERSION
            )
            set(expected_varname "case${caseid}_expected_PY_${varname}")
            set(current_varname "PY_${varname}")
            if(NOT "${${expected_varname}}" STREQUAL "${${current_varname}}")
                message("input: ${input}")
                display_output_values()
                message(FATAL_ERROR "error: case ${caseid}: \n  expected: ${expected_varname} [${${expected_varname}}]\n   current: ${current_varname} [${${current_varname}}]")
            endif()
        endforeach()
    endforeach()

    message("SUCCESS")
endfunction()

if(TEST_python_extract_version_info)
    python_extract_version_info_test()
endif()
