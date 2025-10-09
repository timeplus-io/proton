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

set(BENCHMARK_LIBRARY
    bson/bson_decoding.hpp
    bson/bson_encoding.hpp
    multi_doc/find_many.hpp
    multi_doc/gridfs_download.hpp
    multi_doc/gridfs_upload.hpp
    multi_doc/bulk_insert.hpp
    parallel/gridfs_multi_export.hpp
    parallel/gridfs_multi_import.hpp
    parallel/json_multi_import.hpp
    parallel/json_multi_export.hpp
    single_doc/find_one_by_id.hpp
    single_doc/insert_one.hpp
    single_doc/run_command.hpp
    benchmark_runner.cpp
    main.cpp
    microbench.cpp
    score_recorder.cpp
)

file (GLOB benchmark_DIST_hpps RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.hpp)

set_dist_list (benchmark_DIST
   CMakeLists.txt
   README.md
   ${BENCHMARK_LIBRARY}
   ${benchmark_DIST_hpps}
)

add_executable(microbenchmarks ${BENCHMARK_LIBRARY})
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(microbenchmarks PRIVATE Threads::Threads)

if(MONGOCXX_BUILD_SHARED)
    target_link_libraries(microbenchmarks PRIVATE mongocxx_shared)
endif()

if(MONGOCXX_BUILD_STATIC)
    target_link_libraries(microbenchmarks PRIVATE mongocxx_static)
endif()
