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

add_definitions(-DMONGO_CXX_DRIVER_COMPILING)
add_subdirectory(bsoncxx)
add_subdirectory(mongocxx)

set_local_dist(src_DIST_local
    CMakeLists.txt
    third_party/catch/main.cpp
    third_party/catch/include/catch.hpp
    third_party/catch/include/helpers.hpp
)

set(src_DIST
    ${src_DIST_local}
    ${src_bsoncxx_DIST}
    ${src_mongocxx_DIST}
    PARENT_SCOPE
)
