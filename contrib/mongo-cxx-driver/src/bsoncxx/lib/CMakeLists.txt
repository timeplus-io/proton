# Copyright 2023 MongoDB Inc.
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

list(APPEND bsoncxx_sources
    bsoncxx/v_noabi/bsoncxx/array/element.cpp
    bsoncxx/v_noabi/bsoncxx/array/value.cpp
    bsoncxx/v_noabi/bsoncxx/array/view.cpp
    bsoncxx/v_noabi/bsoncxx/builder/core.cpp
    bsoncxx/v_noabi/bsoncxx/decimal128.cpp
    bsoncxx/v_noabi/bsoncxx/document/element.cpp
    bsoncxx/v_noabi/bsoncxx/document/value.cpp
    bsoncxx/v_noabi/bsoncxx/document/view.cpp
    bsoncxx/v_noabi/bsoncxx/exception/error_code.cpp
    bsoncxx/v_noabi/bsoncxx/json.cpp
    bsoncxx/v_noabi/bsoncxx/oid.cpp
    bsoncxx/v_noabi/bsoncxx/private/itoa.cpp
    bsoncxx/v_noabi/bsoncxx/string/view_or_value.cpp
    bsoncxx/v_noabi/bsoncxx/types.cpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/value.cpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/view.cpp
    bsoncxx/v_noabi/bsoncxx/validate.cpp
)
list(TRANSFORM bsoncxx_sources PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
set(bsoncxx_sources "${bsoncxx_sources}" PARENT_SCOPE)

# Generate private headers.
if(1)
    configure_file(
        bsoncxx/v_noabi/bsoncxx/config/private/config.hh.in
        bsoncxx/v_noabi/bsoncxx/config/private/config.hh
    )
endif()

# Generate and install public headers.
if(1)
    configure_file(
        bsoncxx/v_noabi/bsoncxx/config/config.hpp.in
        bsoncxx/v_noabi/bsoncxx/config/config.hpp
    )

    configure_file(
        bsoncxx/v_noabi/bsoncxx/config/version.hpp.in
        bsoncxx/v_noabi/bsoncxx/config/version.hpp
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/bsoncxx/v_noabi/bsoncxx/config/config.hpp
        ${CMAKE_CURRENT_BINARY_DIR}/bsoncxx/v_noabi/bsoncxx/config/version.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/bsoncxx/v_noabi/bsoncxx/config
        COMPONENT dev
    )
endif()

set_dist_list(src_bsoncxx_lib_DIST
    CMakeLists.txt
    bsoncxx/v_noabi/bsoncxx/array/element.cpp
    bsoncxx/v_noabi/bsoncxx/array/value.cpp
    bsoncxx/v_noabi/bsoncxx/array/view.cpp
    bsoncxx/v_noabi/bsoncxx/builder/core.cpp
    bsoncxx/v_noabi/bsoncxx/config/config.hpp.in
    bsoncxx/v_noabi/bsoncxx/config/private/config.hh.in
    bsoncxx/v_noabi/bsoncxx/config/private/postlude.hh
    bsoncxx/v_noabi/bsoncxx/config/private/prelude.hh
    bsoncxx/v_noabi/bsoncxx/config/version.hpp.in
    bsoncxx/v_noabi/bsoncxx/decimal128.cpp
    bsoncxx/v_noabi/bsoncxx/document/element.cpp
    bsoncxx/v_noabi/bsoncxx/document/value.cpp
    bsoncxx/v_noabi/bsoncxx/document/view.cpp
    bsoncxx/v_noabi/bsoncxx/exception/error_code.cpp
    bsoncxx/v_noabi/bsoncxx/json.cpp
    bsoncxx/v_noabi/bsoncxx/oid.cpp
    bsoncxx/v_noabi/bsoncxx/private/b64_ntop.hh
    bsoncxx/v_noabi/bsoncxx/private/helpers.hh
    bsoncxx/v_noabi/bsoncxx/private/itoa.cpp
    bsoncxx/v_noabi/bsoncxx/private/itoa.hh
    bsoncxx/v_noabi/bsoncxx/private/libbson.hh
    bsoncxx/v_noabi/bsoncxx/private/stack.hh
    bsoncxx/v_noabi/bsoncxx/private/suppress_deprecation_warnings.hh
    bsoncxx/v_noabi/bsoncxx/string/view_or_value.cpp
    bsoncxx/v_noabi/bsoncxx/test_util/export_for_testing.hh
    bsoncxx/v_noabi/bsoncxx/types.cpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/private/value.hh
    bsoncxx/v_noabi/bsoncxx/types/bson_value/value.cpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/view.cpp
    bsoncxx/v_noabi/bsoncxx/types/private/convert.hh
    bsoncxx/v_noabi/bsoncxx/validate.cpp
)
