# Copyright 2020 MongoDB Inc.
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

# Demonstrates how to use the CMake 'add_subdirectory' mechanism to locate and
# build against the libmongoc and libmongocxx shared libraries.

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

set(BUILD_SHARED_AND_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(ENABLE_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(mongo-c-driver)
add_subdirectory(mongo-cxx-driver)

add_executable(hello_mongocxx hello_mongocxx.cpp)

target_link_libraries(hello_mongocxx
  PRIVATE mongocxx_static
)

add_custom_target(run
    COMMAND hello_mongocxx
    DEPENDS hello_mongocxx
    WORKING_DIRECTORY ${CMAKE_PROJECT_DIR}
)
