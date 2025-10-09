# Use FetchContent to obtain mnmlstc/core.

include(FetchContent)

set(core-src "${CMAKE_CURRENT_BINARY_DIR}/_deps/core-src")
set(core-subbuild "${CMAKE_CURRENT_BINARY_DIR}/_deps/core-subbuild")
set(core-build "${CMAKE_CURRENT_BINARY_DIR}/_deps/core-build")
set(core-install "${CMAKE_CURRENT_BINARY_DIR}/_deps/core-install")

# Also update etc/purls.txt.
FetchContent_Declare(
    EP_mnmlstc_core

    SOURCE_DIR "${core-src}"
    SUBBUILD_DIR "${core-subbuild}"
    BINARY_DIR "${core-build}"
    INSTALL_DIR "${core-install}"

    GIT_REPOSITORY https://github.com/mnmlstc/core
    GIT_TAG v1.1.0
    GIT_SHALLOW TRUE
    LOG_DOWNLOAD ON

    FETCHCONTENT_UPDATES_DISCONNECTED ON
)
FetchContent_GetProperties(EP_mnmlstc_core)

find_package(core NO_DEFAULT_PATH PATHS "${core-install}" QUIET)

if(core_FOUND AND "$CACHE{INTERNAL_MONGOC_MNMLSTC_CORE_FOUND}")
# The mnmlstc/core library is already populated and up-to-date.
else()
    if(NOT ep_mnmlstc_core_POPULATED)
        message(STATUS "Downloading mnmlstc/core...")
        FetchContent_Populate(EP_mnmlstc_core)
        message(STATUS "Downloading mnmlstc/core... done.")
    endif()

    message(STATUS "Configuring mnmlstc/core...")
    execute_process(
        COMMAND
        "${CMAKE_COMMAND}"
        "-S" "${core-src}"
        "-B" "${core-build}"
        "-DCMAKE_INSTALL_PREFIX=${core-install}"
        "-DBUILD_TESTING=OFF"
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
        RESULT_VARIABLE retval
    )

    if(NOT "${retval}" STREQUAL "0")
        message(FATAL_ERROR "execute_process() fatal error: ${retval}")
    endif()

    message(STATUS "Configuring mnmlstc/core... done.")

    message(STATUS "Building mnmlstc/core...")
    execute_process(
        COMMAND
        "${CMAKE_COMMAND}"
        --build "${core-build}"
        --config "${CMAKE_BUILD_TYPE}"
        RESULT_VARIABLE retval
    )

    if(NOT "${retval}" STREQUAL "0")
        message(FATAL_ERROR "execute_process() fatal error: ${retval}")
    endif()

    message(STATUS "Building mnmlstc/core... done.")

    message(STATUS "Installing mnmlstc/core...")
    execute_process(
        COMMAND
        "${CMAKE_COMMAND}"
        --install "${core-build}"
        --config "${CMAKE_BUILD_TYPE}"
        RESULT_VARIABLE retval
    )

    if(NOT "${retval}" STREQUAL "0")
        message(FATAL_ERROR "execute_process() fatal error: ${retval}")
    endif()

    message(STATUS "Installing mnmlstc/core... done.")

    find_package(core REQUIRED NO_DEFAULT_PATH PATHS "${core-install}")

    # Ensure the mnmlstc/core CMake configuration is also (re)configured if the cache is fresh.
    set(INTERNAL_MONGOC_MNMLSTC_CORE_FOUND TRUE CACHE INTERNAL "")
endif()

set(CORE_INCLUDE_DIR "${CORE_INCLUDE_DIR}" PARENT_SCOPE)

install(
    DIRECTORY "${core-install}/include/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bsoncxx/v_noabi/bsoncxx/third_party/mnmlstc"
)
