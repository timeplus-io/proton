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

file (GLOB etc_DIST_cmds RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cmd)
file (GLOB etc_DIST_csss RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.css)
file (GLOB etc_DIST_jsons RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.json)
file (GLOB etc_DIST_mds RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.md)
file (GLOB etc_DIST_pls RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.pl)
file (GLOB etc_DIST_pys RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.py)
file (GLOB etc_DIST_shs RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.sh)
file (GLOB etc_DIST_txts RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.txt)

set_dist_list (etc_DIST
   CMakeLists.txt
   ${etc_DIST_cmds}
   ${etc_DIST_csss}
   ${etc_DIST_jsons}
   ${etc_DIST_mds}
   ${etc_DIST_pls}
   ${etc_DIST_pys}
   ${etc_DIST_shs}
   ${etc_DIST_txts}
   memcheck.suppressions
   ubsan.ignorelist
)
