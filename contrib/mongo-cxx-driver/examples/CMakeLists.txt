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

# NOTE: The targets in this directory will only build if you have
# installed the bsoncxx and mongocxx libraries via the 'install'
# targets.

# TODO: Ideally, there would be dependencies here on the install targets
# for bsoncxx and mongocxx. However, there is currently no way to express
# that dependency in CMake (see https://cmake.org/Bug/view.php?id=8438)

# Build the examples with vectorcall as the default on Windows
# so that we shake out missing _CALL macros.
if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Gv")
endif()

add_subdirectory(bsoncxx)
add_subdirectory(mongocxx)
add_subdirectory(projects)

add_custom_target(examples DEPENDS ${MONGOCXX_EXAMPLE_EXECUTABLES} ${BSONCXX_EXAMPLE_EXECUTABLES})

add_custom_target(run-examples DEPENDS examples)

# Run all mongocxx examples on `make run-examples`.
foreach(EXAMPLE ${MONGOCXX_EXAMPLE_EXECUTABLES})
    # Skip the "exception" example when running with Visual Studio, which currently fails due to the
    # following CMake/MSBuild issue:
    # <https://cmake.org/pipermail/cmake-developers/2015-October/026775.html>.  The "exception"
    # example prints "error:" to standard out, which MSBuild interprets as a command failure due to
    # its default behavior of detecting "standard error format" messages from commands.
    if ((${EXAMPLE} STREQUAL "exception") AND (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC"))
        continue()
    endif()

    # TODO CXX-1201, enable some examples only for replica sets.
    if (${EXAMPLE} MATCHES "change_streams|client_session|with_transaction|causal_consistency")
        continue()
    endif()

    # Don't run the CSFLE examples as part of the suite; needs mongocryptd running.
    if (${EXAMPLE} MATCHES "encryption")
        continue()
    endif()

    add_custom_command(TARGET run-examples POST_BUILD COMMAND ${EXAMPLE})
endforeach(EXAMPLE)

# Run all bsoncxx examples on `make run-examples`.
foreach(EXAMPLE ${BSONCXX_EXAMPLE_EXECUTABLES})
    add_custom_command(TARGET run-examples POST_BUILD COMMAND ${EXAMPLE})
endforeach(EXAMPLE)

file (GLOB examples_DIST_mds RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.md)

set_local_dist (examples_DIST_local
   CMakeLists.txt
   ${examples_DIST_mds}
)

set (examples_DIST
   ${examples_DIST_local}
   ${examples_bsoncxx_DIST}
   ${examples_mongocxx_DIST}
   ${examples_projects_DIST}
   PARENT_SCOPE
)
