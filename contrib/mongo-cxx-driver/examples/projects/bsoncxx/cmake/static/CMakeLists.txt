# Copyright 2017 MongoDB Inc.
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

# Demonstrates how to use the CMake 'find_package' mechanism to locate build against the libbsoncxx
# static library.

cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

if(POLICY CMP0025)
    cmake_policy(SET CMP0025 NEW)
endif()

project(HELLO_WORLD LANGUAGES C CXX)

# Enforce the C++ standard, and disable extensions.
if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()

set(CMAKE_CXX_EXTENSIONS OFF)

if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
endif()

# NOTE: For this to work, the CMAKE_PREFIX_PATH variable must be set to point to the directory that
# was used as the argument to CMAKE_INSTALL_PREFIX when building libbsoncxx.
find_package(bsoncxx REQUIRED)

add_executable(hello_bsoncxx ../../hello_bsoncxx.cpp)

target_link_libraries(hello_bsoncxx
  PRIVATE mongo::bsoncxx_static
)

target_compile_definitions(hello_bsoncxx
  PRIVATE ${LIBBSONCXX_STATIC_DEFINITIONS}
)

add_custom_target(run
    COMMAND hello_bsoncxx
    DEPENDS hello_bsoncxx
    WORKING_DIRECTORY ${CMAKE_PROJECT_DIR}
)

# Sanity-check that static library macros are set when building against the static library.  Users
# don't need to include this section in their projects.
get_target_property(LIBBSONCXX_STATIC_DEFINITIONS mongo::bsoncxx_static INTERFACE_COMPILE_DEFINITIONS)
list(FIND LIBBSONCXX_STATIC_DEFINITIONS "BSONCXX_STATIC" LIST_IDX)
if (${LIST_IDX} EQUAL -1)
    message(FATAL_ERROR "Expected BSONCXX_STATIC to be defined")
endif()
