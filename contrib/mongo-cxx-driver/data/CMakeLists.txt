# Copyright 2018-present MongoDB Inc.
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

file (GLOB_RECURSE data_DIST_jsons RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.json)
file (GLOB_RECURSE data_DIST_rsts RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.rst)
file (GLOB_RECURSE data_DIST_txts RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.txt)

set_dist_list (data_DIST
   CMakeLists.txt
   ${data_DIST_jsons}
   ${data_DIST_rsts}
   ${data_DIST_txts}
)
