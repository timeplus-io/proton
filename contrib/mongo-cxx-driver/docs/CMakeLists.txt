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

add_subdirectory (themes)

file (GLOB_RECURSE docs_DIST_mds RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} content/*.md)
file (GLOB docs_DIST_htmls RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} layouts/shortcodes/*.html)
file (GLOB docs_DIST_tomls RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.toml data/*.toml)

set_local_dist (docs_DIST_local
   CMakeLists.txt
   ${docs_DIST_mds}
   ${docs_DIST_htmls}
   ${docs_DIST_tomls}
   layouts/.readme
   static/.readme
)

set (docs_DIST
   ${docs_DIST_local}
   ${docs_themes_DIST}
   PARENT_SCOPE
)
