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

# Demonstrates how to use the CMake 'find_package' mechanism to locate
# and build against libmongoc.

cmake_minimum_required (VERSION 3.0)

if (APPLE)
   cmake_policy (SET CMP0042 OLD)
endif ()

project (hello_mongoc LANGUAGES C)

# NOTE: For this to work, the CMAKE_PREFIX_PATH variable must be set to point to
# the directory that was used as the argument to CMAKE_INSTALL_PREFIX when
# building libmongoc.
# -- sphinx-include-start --
# Specify the minimum version you require.
find_package (mongoc-1.0 1.7 REQUIRED)

# The "hello_mongoc.c" sample program is shared among four tests.
add_executable (hello_mongoc ../../hello_mongoc.c)
target_link_libraries (hello_mongoc PRIVATE mongo::mongoc_shared)
