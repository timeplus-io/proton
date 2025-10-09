cmake_policy(VERSION 3.15)

if(NOT CMAKE_SCRIPT_MODE_FILE)
    # We are being included from within a project, so we should generate the install rules
    # The script name is "uninstall" by default:
    if(NOT DEFINED UNINSTALL_SCRIPT_NAME)
        set(UNINSTALL_SCRIPT_NAME "uninstall")
    endif()
    # We need a directory where we should install the script:
    if(NOT UNINSTALL_PROG_DIR)
        message(SEND_ERROR "We require an UNINSTALL_PROG_DIR to be defined")
    endif()
    # Platform dependent values:
    if(WIN32)
        set(_script_ext "cmd")
        set(_script_runner cmd.exe /c)
    else()
        set(_script_ext "sh")
        set(_script_runner sh -e -u)
    endif()
    # The script filename and path:
    set(_script_filename "${UNINSTALL_SCRIPT_NAME}.${_script_ext}")
    get_filename_component(_uninstaller_script "${CMAKE_CURRENT_BINARY_DIR}/${_script_filename}" ABSOLUTE)
    # Code that will do the work at install-time:
    string(CONFIGURE [==[
    function(__generate_uninstall)
        set(UNINSTALL_IS_WIN32 "@WIN32@")
        set(UNINSTALL_WRITE_FILE "@_uninstaller_script@")
        set(UNINSTALL_SCRIPT_SELF "@UNINSTALL_PROG_DIR@/@_script_filename@")
        include("@CMAKE_CURRENT_LIST_FILE@")
    endfunction()
    __generate_uninstall()
    ]==] code @ONLY ESCAPE_QUOTES)
    install(CODE "${code}")
    # Add a rule to install that file:
    install(
        FILES "${_uninstaller_script}"
        DESTINATION "${UNINSTALL_PROG_DIR}"
        PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE
        )

    # If applicable, generate an "uninstall" target to run the uninstaller:
    if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR OR PROJECT_IS_TOP_LEVEL)
        add_custom_target(
            uninstall
            COMMAND ${_script_runner} "${_uninstaller_script}"
            COMMENT Uninstalling...
        )
    endif()
    # Stop here: The rest of the file is for install-time
    return()
endif()

# We get here if running in script mode (e.g. at CMake install-time)
if(NOT DEFINED CMAKE_INSTALL_MANIFEST_FILES)
    message(FATAL_ERROR "This file is only for use with CMake's install(CODE/SCRIPT) command")
endif()
if(NOT DEFINED UNINSTALL_WRITE_FILE)
    message(FATAL_ERROR "Expected a variable “UNINSTALL_WRITE_FILE” to be defined")
endif()

# Clear out the uninstall script before we begin writing:
file(WRITE "${UNINSTALL_WRITE_FILE}" "")

# Append a line to the uninstall script file. Single quotes will be replaced with doubles,
# and an appropriate newline will be added.
function(append_line line)
    string(REPLACE "'" "\"" line "${line}")
    file(APPEND "${UNINSTALL_WRITE_FILE}" "${line}\n")
endfunction()

# The copyright header:
set(header [[
Mongo C Driver uninstall program, generated with CMake

Copyright 2018-present MongoDB, Inc.

Licensed under the Apache License, Version 2.0 (the \"License\");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an \"AS IS\" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
]])
string(STRIP header "${header}")
string(REPLACE "\n" ";" header_lines "${header}")

# Prefix for the Batch script:
set(bat_preamble [[
call :init

:print
<nul set /p_=%~1
exit /b

:rmfile
set f=%__prefix%\%~1
call :print "Remove file %f% "
if EXIST "%f%" (
    del /Q /F "%f%" || exit /b %errorlevel%
    call :print " - ok"
) else (
    call :print " - skipped: not present"
)
echo(
exit /b

:rmdir
set f=%__prefix%\%~1
call :print "Remove directory: %f% "
if EXIST "%f%" (
    rmdir /Q "%f%" 2>nul
    if ERRORLEVEL 0 (
        call :print "- ok"
    ) else (
        call :print "- skipped (non-empty?)"
    )
) else (
    call :print " - skipped: not present"
)
echo(
exit /b

:init
setlocal EnableDelayedExpansion
setlocal EnableExtensions
if /i "%~dp0" NEQ "%TEMP%\" (
    set tmpfile=%TEMP%\mongoc-%~nx0
    copy "%~f0" "!tmpfile!" >nul
    call "!tmpfile!" & del "!tmpfile!"
    exit /b
)
]])

# Prefix for the shell script:
set(sh_preamble [[
set -eu

__rmfile() {
    set -eu
    abs=$__prefix/$1
    printf "Remove file %s: " "$abs"
    if test -f "$abs" || test -L "$abs"
    then
        rm -- "$abs"
        echo "ok"
    else
        echo "skipped: not present"
    fi
}

__rmdir() {
    set -eu
    abs=$__prefix/$1
    printf "Remove directory %s: " "$abs"
    if test -d "$abs"
    then
        list=$(ls --almost-all "$abs")
        if test "$list" = ""
        then
            rmdir -- "$abs"
            echo "ok"
        else
            echo "skipped: not empty"
        fi
    else
        echo "skipped: not present"
    fi
}
]])

# Convert the install prefix to an absolute path with the native path format:
get_filename_component(install_prefix "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)
file(TO_NATIVE_PATH "${install_prefix}" install_prefix)
# Handling DESTDIR requires careful handling of root path redirection:
set(root_path)
set(relative_prefix "${install_prefix}")
if(COMMAND cmake_path)
    cmake_path(GET install_prefix ROOT_PATH root_path)
    cmake_path(GET install_prefix RELATIVE_PART relative_prefix)
endif()

# The first lines that will be written to the script:
set(init_lines)

if(UNINSTALL_IS_WIN32)
    # Comment the header:
    list(TRANSFORM header_lines PREPEND "rem ")
    # Add the preamble
    list(APPEND init_lines
        "@echo off"
        "${header_lines}"
        "${bat_preamble}"
        "if \"%DESTDIR%\"==\"\" ("
        "  set __prefix=${install_prefix}"
        ") else ("
        "  set __prefix=!DESTDIR!\\${relative_prefix}"
        ")"
        "")
    set(__rmfile "call :rmfile")
    set(__rmdir "call :rmdir")
else()
    # Comment the header:
    list(TRANSFORM header_lines PREPEND "# * ")
    # Add the preamble
    list(APPEND init_lines
        "#!/bin/sh"
        "${header_lines}"
        "${sh_preamble}"
        "__prefix=\${DESTDIR:-}${install_prefix}"
        "")
    set(__rmfile "__rmfile")
    set(__rmdir "__rmdir")
endif()

# Add the first lines to the file:
string(REPLACE ";" "\n" init "${init_lines}")
append_line("${init}")

# Generate a "remove a file" command
function(add_rmfile filename)
    file(TO_NATIVE_PATH "${filename}" native)
    append_line("${__rmfile} '${native}'")
endfunction()

# Generate a "remove a directory" command
function(add_rmdir dirname)
    file(TO_NATIVE_PATH "${dirname}" native)
    append_line("${__rmdir} '${native}'")
endfunction()

set(script_self "${install_prefix}/${UNINSTALL_SCRIPT_SELF}")
set(dirs_to_remove)
foreach(installed IN LISTS CMAKE_INSTALL_MANIFEST_FILES script_self)
    # Get the relative path from the prefix (the uninstaller will fix it up later)
    file(RELATIVE_PATH relpath "${install_prefix}" "${installed}")
    # Add a removal:
    add_rmfile("${relpath}")
    # Climb the path and collect directories:
    while("1")
        get_filename_component(installed "${installed}" DIRECTORY)
        file(TO_NATIVE_PATH "${installed}" installed)
        get_filename_component(parent "${installed}" DIRECTORY)
        file(TO_NATIVE_PATH "${parent}" parent)
        # Don't account for the prefix or direct children of the prefix:
        if(installed STREQUAL install_prefix OR parent STREQUAL install_prefix)
            break()
        endif()
        # Keep track of this directory for later:
        list(APPEND dirs_to_remove "${installed}")
    endwhile()
endforeach()

# Now generate commands to remove (empty) directories:
list(REMOVE_DUPLICATES dirs_to_remove)
# Order them by depth so that we remove subdirectories before their parents:
list(SORT dirs_to_remove ORDER DESCENDING)
foreach(dir IN LISTS dirs_to_remove)
    file(RELATIVE_PATH relpath "${install_prefix}" "${dir}")
    add_rmdir("${relpath}")
endforeach()

message(STATUS "Generated uninstaller: ${UNINSTALL_WRITE_FILE}")
