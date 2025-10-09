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

add_subdirectory (mongodb)

set_local_dist (docs_themes_DIST_local
   CMakeLists.txt
)

set (docs_themes_DIST
   ${docs_themes_DIST_local}
   ${docs_themes_mongodb_DIST}
   PARENT_SCOPE
)
