#[[

Searches for a Cyrus "libsasl2" library available on the system.

Upon success, Defines an imported target `SASL2::SASL2` that can be linked into
other targts.

]]

include(FindPackageHandleStandardArgs)

# Upon early return, tell the caller that we don't have it:
set(SASL2_FOUND FALSE)

if(SASL2_FIND_COMPONENTS)
    message(FATAL_ERROR "This find_package(SASL2) does not support package components (Got “${SASL2_FIND_COMPONENTS}”)")
endif()

list(APPEND SASL2_PATHS C:/sasl)

# Search for the proper qualified path <sasl/sasl.h>, not sasl.h
find_path(
    SASL2_INCLUDE_DIR "sasl/sasl.h"
    DOC "Header include-directory for Cyrus libsasl2"
    HINTS ${SASL2_HINTS}
    PATHS ${SASL2_PREFIX} ${SASL2_ROOT_DIR} ${SASL2_PATHS}
    PATH_SUFFIXES include
)

# Use the header path as a hint for the library path:
unset(_hint)
if(SASL2_INCLUDE_DIR)
    get_filename_component(_hint "${SASL2_INCLUDE_DIR}" DIRECTORY)
endif()

# The library filename is libsasl2.so, libsasl.dylib, etc.
find_library(
    SASL2_LIBRARY sasl2
    DOC "Library file for Cyrus libsasl2"
    HINTS ${_hint} ${SASL2_HINTS}
    PATHS ${SASL2_PREFIX} ${SASL2_ROOT_DIR} ${SASL2_PATHS}
    PATH_SUFFIXES lib
)

if(SASL2_INCLUDE_DIR)
    message(DEBUG "Found SASL2 include-dir: ${SASL2_INCLUDE_DIR}")
    # Extract the library version from the sasl.h header file:
    file(READ "${SASL2_INCLUDE_DIR}/sasl/sasl.h" _sasl_h)
    # It is defined via three macro definitions:
    string(CONCAT _version_regex
        "define[ \t]+SASL_VERSION_MAJOR[ \t]+([0-9]+).+"
        "define[ \t]+SASL_VERSION_MINOR[ \t]+([0-9]+).+"
        "define[ \t]+SASL_VERSION_STEP[ \t]+([0-9]+)"
        )
    if(NOT _sasl_h MATCHES "${_version_regex}")
        # Very strange...
        set(SASL2_NOT_FOUND_MESSAGE [[A sasl/sasl.h file was found, but we could not extract version information]])
    else()
        set(SASL2_VERSION "${CMAKE_MATCH_1}.${CMAKE_MATCH_2}.${CMAKE_MATCH_3}")
        message(DEBUG "Found libsasl2 version from sasl.h: ${SASL2_VERSION}")
    endif()
endif()

find_package_handle_standard_args(SASL2
    REQUIRED_VARS SASL2_VERSION SASL2_INCLUDE_DIR SASL2_LIBRARY
    VERSION_VAR SASL2_VERSION
)

if(NOT SASL2_FOUND)
    return()
endif()

message(DEBUG "Found SASL2 library: ${SASL2_LIBRARY}")

# Generate an imported target based on the paths that we found.
if(NOT TARGET SASL2::SASL2)
    # (Guard against double-import)
    add_library(SASL2::SASL2 IMPORTED UNKNOWN GLOBAL)
endif()
set_target_properties(SASL2::SASL2 PROPERTIES
    IMPORTED_LOCATION "${SASL2_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${SASL2_INCLUDE_DIR}"
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${SASL2_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES ""  # Clear this property in case of double-import
    VERSION "${SASL2_VERSION}"
)

# libsasl2 requires dlopen():
set_property(TARGET SASL2::SASL2 APPEND PROPERTY INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS})
