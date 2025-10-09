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

set_dist_list (examples_projects_DIST
   CMakeLists.txt
   bsoncxx/cmake/shared/CMakeLists.txt
   bsoncxx/cmake/shared/build.sh
   bsoncxx/cmake/shared/build/.gitignore
   bsoncxx/cmake/static/CMakeLists.txt
   bsoncxx/cmake/static/build.sh
   bsoncxx/cmake/static/build/.gitignore
   bsoncxx/hello_bsoncxx.cpp
   bsoncxx/pkg-config/shared/build.sh
   bsoncxx/pkg-config/shared/build/.gitignore
   bsoncxx/pkg-config/static/build.sh
   bsoncxx/pkg-config/static/build/.gitignore
   mongocxx/cmake/shared/CMakeLists.txt
   mongocxx/cmake/shared/build.sh
   mongocxx/cmake/shared/build/.gitignore
   mongocxx/cmake/static/CMakeLists.txt
   mongocxx/cmake/static/build.sh
   mongocxx/cmake/static/build/.gitignore
   mongocxx/hello_mongocxx.cpp
   mongocxx/pkg-config/shared/build.sh
   mongocxx/pkg-config/shared/build/.gitignore
   mongocxx/pkg-config/static/build.sh
   mongocxx/pkg-config/static/build/.gitignore
)
