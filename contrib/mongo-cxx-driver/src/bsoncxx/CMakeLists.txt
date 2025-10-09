# Copyright 2016 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(BSONCXX)

ParseVersion(${BUILD_VERSION} BSONCXX)

set(BSONCXX_VERSION_NO_EXTRA ${BSONCXX_VERSION_MAJOR}.${BSONCXX_VERSION_MINOR}.${BSONCXX_VERSION_PATCH})
set(BSONCXX_VERSION ${BSONCXX_VERSION_NO_EXTRA}${BSONCXX_VERSION_EXTRA})
message(STATUS "bsoncxx version: ${BSONCXX_VERSION}")

option(BSONCXX_POLY_USE_IMPLS "Use bsoncxx implementations for stdx polyfills" OFF)
option(BSONCXX_POLY_USE_MNMLSTC "Use MNMLSTC/core for stdx polyfills" OFF)
option(BSONCXX_POLY_USE_SYSTEM_MNMLSTC "Obtain mnmlstc/core from system" OFF)
option(BSONCXX_POLY_USE_BOOST "Use boost for stdx polyfills" OFF)
option(BSONCXX_POLY_USE_STD "Use C++17 std library for stdx polyfills" OFF)

set(BSONCXX_OUTPUT_BASENAME "bsoncxx" CACHE STRING "Output bsoncxx library base name")

# Count how many polyfill options are true-ish
set(BSONCXX_POLY_OPTIONS_SET 0)

foreach(BSONCXX_POLY_OPTION ${BSONCXX_POLY_USE_IMPLS} ${BSONCXX_POLY_USE_MNMLSTC} ${BSONCXX_POLY_USE_BOOST} ${BSONCXX_POLY_USE_STD})
    if(${BSONCXX_POLY_OPTION})
        MATH(EXPR BSONCXX_POLY_OPTIONS_SET "${BSONCXX_POLY_OPTIONS_SET}+1")
    endif()
endforeach()

if(BSONCXX_POLY_OPTIONS_SET GREATER 1)
    # You can't ask for more than one polyfill
    message(FATAL_ERROR "Cannnot specify more than one bsoncxx polyfill choice")
elseif(BSONCXX_POLY_OPTIONS_SET EQUAL 0)
    # You can just not say, in which case we endeavor to pick a sane default:
    if(NOT CMAKE_CXX_STANDARD LESS 17)
        # If we are in C++17 mode, use the C++17 versions
        set(BSONCXX_POLY_USE_STD ON)
        message(STATUS "Auto-configuring bsoncxx to use C++17 std library polyfills since C++17 is active and user didn't specify otherwise")
    elseif(ENABLE_BSONCXX_POLY_USE_IMPLS)
        # If enabled, use bsoncxx implementations instead of external libraries.
        set(BSONCXX_POLY_USE_IMPLS ON)
        message(STATUS "Auto-configuring bsoncxx to use its own polyfill implementations since C++17 is inactive")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        # Otherwise, since MSVC can't handle MNMLSTC, default to boost
        set(BSONCXX_POLY_USE_BOOST ON)
        message(STATUS "Auto-configuring bsoncxx to use boost std library polyfills since C++17 is inactive and compiler is MSVC")
        message(DEPRECATION "Default selection of external polyfill libraries is deprecated and will be removed in an upcoming major release. Set ENABLE_BSONCXX_POLY_USE_IMPLS=ON to enable recommended behavior.")
    else()
        # Otherwise, we are on a platform that can handle MNMLSTC
        set(BSONCXX_POLY_USE_MNMLSTC ON)
        message(STATUS "Auto-configuring bsoncxx to use MNMLSTC for polyfills since C++17 is inactive")
        message(DEPRECATION "Default selection of external polyfill libraries is deprecated and will be removed in an upcoming major release. Set ENABLE_BSONCXX_POLY_USE_IMPLS=ON to enable recommended behavior.")
    endif()
elseif(BSONCXX_POLY_USE_MNMLSTC OR BSONCXX_POLY_USE_BOOST)
    message(DEPRECATION "Support for external polyfill libraries is deprecated and will be removed in an upcoming major release. Set BSONCXX_POLY_USE_IMPLS=ON (pre-C++17) or set CMAKE_CXX_STANDARD=17 (or newer) to select recommended behavior.")
endif()

# It doesn't make sense to say we aren't using MNMLSTC but then
# request the system version of it.
if(NOT BSONCXX_POLY_USE_MNMLSTC AND BSONCXX_POLY_USE_SYSTEM_MNMLSTC)
    MESSAGE(FATAL_ERROR "Can't specify system mnmlstc when not using mnmlstc")
endif()

set(BSONCXX_BOOST_PKG_DEP "") # Required by bsoncxx-config.cmake.in.

if(BSONCXX_POLY_USE_BOOST)
    set(BSONCXX_BOOST_PKG_DEP "find_dependency(Boost 1.56.0 REQUIRED)")
endif()

set(BSONCXX_PKG_DEP "") # Required by bsoncxx-config.cmake.in.

if(TARGET bson_shared OR TARGET bson_static)
    # If these targets exist, then libbson has already been included as a project
    # sub-directory
    message(STATUS "found libbson targets declared in current build scope; version not checked")

    if(NOT BSONCXX_LINK_WITH_STATIC_MONGOC)
        set(libbson_target bson_shared)
    else()
        set(libbson_target bson_static)
    endif()

    set(BSONCXX_PKG_DEP "find_dependency(bson-${LIBBSON_REQUIRED_ABI_VERSION} REQUIRED)")
else()
    # Attempt to find libbson by new package name (without lib).
    find_package(bson-${LIBBSON_REQUIRED_ABI_VERSION} ${LIBBSON_REQUIRED_VERSION} QUIET)

    if(bson-${LIBBSON_REQUIRED_ABI_VERSION}_FOUND)
        message(STATUS "found libbson version ${bson-${LIBBSON_REQUIRED_ABI_VERSION}_VERSION}")

        if(NOT BSONCXX_LINK_WITH_STATIC_MONGOC)
            set(libbson_target mongo::bson_shared)
        else()
            set(libbson_target mongo::bson_static)
        endif()

        set(BSONCXX_PKG_DEP "find_dependency(bson-${LIBBSON_REQUIRED_ABI_VERSION} REQUIRED)")
    else()
        # Require package of old libbson name (with lib).
        if(NOT BSONCXX_LINK_WITH_STATIC_MONGOC)
            find_package(libbson-${LIBBSON_REQUIRED_ABI_VERSION} ${LIBBSON_REQUIRED_VERSION} REQUIRED)
            message(STATUS "found libbson version ${BSON_VERSION}")
            set(libbson_target ${BSON_LIBRARIES})
            set(libbson_include_directories ${BSON_INCLUDE_DIRS})
            set(libbson_definitions ${BSON_DEFINITIONS})
            set(BSONCXX_PKG_DEP "find_dependency(libbson-${LIBBSON_REQUIRED_ABI_VERSION} REQUIRED)")
        else()
            find_package(libbson-static-${LIBBSON_REQUIRED_ABI_VERSION} ${LIBBSON_REQUIRED_VERSION} REQUIRED)
            message(STATUS "found libbson version ${BSON_STATIC_VERSION}")
            set(libbson_target ${BSON_STATIC_LIBRARIES})
            set(libbson_include_directories ${BSON_STATIC_INCLUDE_DIRS})
            set(libbson_definitions ${BSON_STATIC_DEFINITIONS})
            set(BSONCXX_PKG_DEP "find_dependency(libbson-static-${LIBBSON_REQUIRED_ABI_VERSION} REQUIRED)")
        endif()
    endif()
endif()

set(bsoncxx_sources "") # Required by bsoncxx_add_library().

add_subdirectory(include)
add_subdirectory(lib)
add_subdirectory(third_party)

include(BsoncxxUtil)

set(bsoncxx_target_list "")

if(BSONCXX_BUILD_SHARED)
    bsoncxx_add_library(bsoncxx_shared "${BSONCXX_OUTPUT_BASENAME}" SHARED)
    list(APPEND bsoncxx_target_list bsoncxx_shared)

    if(WIN32)
        # Add resource-definition script for Windows shared library (.dll).
        configure_file(bsoncxx.rc.in bsoncxx.rc)
        target_sources(bsoncxx_shared PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/bsoncxx.rc)
    endif()
endif()

if(BSONCXX_BUILD_STATIC)
    bsoncxx_add_library(bsoncxx_static "${BSONCXX_OUTPUT_BASENAME}-static" STATIC)
    list(APPEND bsoncxx_target_list bsoncxx_static)
endif()

# Generate and install the export header.
if(1)
    function(bsoncxx_install_export_header TARGET)
        generate_export_header(${TARGET}
            BASE_NAME BSONCXX
            EXPORT_MACRO_NAME BSONCXX_API
            NO_EXPORT_MACRO_NAME BSONCXX_PRIVATE
            EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/lib/bsoncxx/v_noabi/bsoncxx/config/export.hpp
            STATIC_DEFINE BSONCXX_STATIC
        )

        install(FILES
            ${PROJECT_BINARY_DIR}/lib/bsoncxx/v_noabi/bsoncxx/config/export.hpp
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/bsoncxx/v_noabi/bsoncxx/config
            COMPONENT dev
        )
    endfunction()

    # Ensure the export header is generated *at most* once.
    # Give priority to the shared library target which sets visibility properties.
    if(BSONCXX_BUILD_SHARED)
        bsoncxx_install_export_header(bsoncxx_shared)
    elseif(BSONCXX_BUILD_STATIC)
        bsoncxx_install_export_header(bsoncxx_static)
    endif()
endif()

add_subdirectory(cmake)

if(ENABLE_TESTS)
    add_subdirectory(test)
endif()

set_local_dist(src_bsoncxx_DIST_local
    CMakeLists.txt
    bsoncxx.rc.in
)

set(src_bsoncxx_DIST
    ${src_bsoncxx_DIST_local}
    ${src_bsoncxx_cmake_DIST}
    ${src_bsoncxx_include_DIST}
    ${src_bsoncxx_lib_DIST}
    ${src_bsoncxx_third_party_DIST}
    ${src_bsoncxx_test_DIST}
    PARENT_SCOPE
)
