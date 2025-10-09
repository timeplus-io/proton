# Copyright 2023 MongoDB Inc.
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

include(CMakePackageConfigHelpers)

# Generate and install modern CMake package config files.
if(1)
    write_basic_package_version_file(
        mongocxx-config-version.cmake
        VERSION ${MONGOCXX_VERSION}
        COMPATIBILITY SameMajorVersion
    )

    configure_file(
        mongocxx-config.cmake.in
        mongocxx-config.cmake
        @ONLY
    )

    install(TARGETS
        ${mongocxx_target_list}
        EXPORT mongocxx_targets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT runtime
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT dev
        INCLUDES DESTINATION
        ${CMAKE_INSTALL_INCLUDEDIR}/mongocxx/v_noabi
        ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(EXPORT mongocxx_targets
        NAMESPACE mongo::
        FILE mongocxx_targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mongocxx-${MONGOCXX_VERSION}
    )

    install(
        FILES
        ${CMAKE_CURRENT_BINARY_DIR}/mongocxx-config-version.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/mongocxx-config.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mongocxx-${MONGOCXX_VERSION}
        COMPONENT Devel
    )
endif()

# Generate and install pkg-config package config files.
if(1)
    function(mongocxx_install_pkg_config TARGET LINK_TYPE)
        get_target_property(output_name ${TARGET} OUTPUT_NAME)

        set(pcfilename "${CMAKE_CURRENT_BINARY_DIR}/lib${output_name}.pc")

        if (ENABLE_ABI_TAG_IN_PKGCONFIG_FILENAMES)
            if(LINK_TYPE STREQUAL "STATIC")
                get_target_property(bsoncxx_name bsoncxx_static OUTPUT_NAME)
                set(is_static 1)
            else()
                get_target_property(bsoncxx_name bsoncxx_shared OUTPUT_NAME)
                set(is_static 0)
            endif()
        else()
            if(LINK_TYPE STREQUAL "STATIC")
                set(bsoncxx_name "$CACHE{BSONCXX_OUTPUT_BASENAME}-static")
                set(is_static 1)
            else()
                set(bsoncxx_name "$CACHE{BSONCXX_OUTPUT_BASENAME}")
                set(is_static 0)
            endif()
        endif()

        add_custom_command(
            OUTPUT ${pcfilename}
            COMMAND ${CMAKE_COMMAND}
                -D "src_dir=${CMAKE_CURRENT_SOURCE_DIR}"
                -D "bin_dir=${CMAKE_CURRENT_BINARY_DIR}"
                -D "prefix=${CMAKE_INSTALL_PREFIX}"
                -D "includedir=${CMAKE_INSTALL_INCLUDEDIR}"
                -D "libdir=${CMAKE_INSTALL_LIBDIR}"
                -D "output_name=${output_name}"
                -D "version=${MONGOCXX_VERSION_NO_EXTRA}"
                -D "is_static=${is_static}"
                -D "bsoncxx_name=${bsoncxx_name}"
                -D "libmongoc_req_abi_ver=${LIBMONGOC_REQUIRED_ABI_VERSION}"
                -D "libmongoc_req_ver=${LIBMONGOC_REQUIRED_VERSION}"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/generate-pc.cmake
            MAIN_DEPENDENCY
                ${CMAKE_CURRENT_SOURCE_DIR}/libmongocxx.pc.in
            DEPENDS
                ${CMAKE_CURRENT_SOURCE_DIR}/generate-pc.cmake
        )

        add_custom_target(generate-lib${TARGET}-pc DEPENDS ${pcfilename})
        add_dependencies(${TARGET} generate-lib${TARGET}-pc)

        if(ENABLE_ABI_TAG_IN_PKGCONFIG_FILENAMES)
            install(FILES ${pcfilename} DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
        else()
            if(is_static)
                set(pkgname "lib${MONGOCXX_OUTPUT_BASENAME}-static.pc")
            else()
                set(pkgname "lib${MONGOCXX_OUTPUT_BASENAME}.pc")
            endif()

            install(
                FILES ${pcfilename}
                DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
                RENAME ${pkgname}
            )
        endif()
    endfunction()

    if(MONGOCXX_BUILD_SHARED)
        mongocxx_install_pkg_config(mongocxx_shared SHARED)
    endif()

    if(MONGOCXX_BUILD_STATIC)
        mongocxx_install_pkg_config(mongocxx_static STATIC)
    endif()
endif()

set_dist_list(src_mongocxx_cmake_DIST
    CMakeLists.txt
    generate-pc.cmake
    libmongocxx.pc.in
    mongocxx-config.cmake.in
)
