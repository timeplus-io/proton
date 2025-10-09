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

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT dev
    FILES_MATCHING PATTERN "*.hpp"
)

set_dist_list(src_bsoncxx_include_DIST
    CMakeLists.txt
    bsoncxx/v_noabi/bsoncxx/array/element-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/array/element.hpp
    bsoncxx/v_noabi/bsoncxx/array/value-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/array/value.hpp
    bsoncxx/v_noabi/bsoncxx/array/view_or_value.hpp
    bsoncxx/v_noabi/bsoncxx/array/view-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/array/view.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/array-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/array.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/document-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/document.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/helpers.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/impl.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/kvp.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/sub_array-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/sub_array.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/sub_document-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/basic/sub_document.hpp
    bsoncxx/v_noabi/bsoncxx/builder/concatenate-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/concatenate.hpp
    bsoncxx/v_noabi/bsoncxx/builder/core-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/core.hpp
    bsoncxx/v_noabi/bsoncxx/builder/list-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/list.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/array_context-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/array_context.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/array-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/array.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/closed_context-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/closed_context.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/document-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/document.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/helpers-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/helpers.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/key_context-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/key_context.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/single_context-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/single_context.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/value_context-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/builder/stream/value_context.hpp
    bsoncxx/v_noabi/bsoncxx/config/compiler.hpp
    bsoncxx/v_noabi/bsoncxx/config/postlude.hpp
    bsoncxx/v_noabi/bsoncxx/config/prelude.hpp
    bsoncxx/v_noabi/bsoncxx/config/util.hpp
    bsoncxx/v_noabi/bsoncxx/decimal128-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/decimal128.hpp
    bsoncxx/v_noabi/bsoncxx/document/element-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/document/element.hpp
    bsoncxx/v_noabi/bsoncxx/document/value-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/document/value.hpp
    bsoncxx/v_noabi/bsoncxx/document/view_or_value.hpp
    bsoncxx/v_noabi/bsoncxx/document/view-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/document/view.hpp
    bsoncxx/v_noabi/bsoncxx/enums/binary_sub_type.hpp
    bsoncxx/v_noabi/bsoncxx/enums/type.hpp
    bsoncxx/v_noabi/bsoncxx/exception/error_code-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/exception/error_code.hpp
    bsoncxx/v_noabi/bsoncxx/exception/exception-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/exception/exception.hpp
    bsoncxx/v_noabi/bsoncxx/json-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/json.hpp
    bsoncxx/v_noabi/bsoncxx/oid-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/oid.hpp
    bsoncxx/v_noabi/bsoncxx/stdx/make_unique.hpp
    bsoncxx/v_noabi/bsoncxx/stdx/operators.hpp
    bsoncxx/v_noabi/bsoncxx/stdx/optional.hpp
    bsoncxx/v_noabi/bsoncxx/stdx/string_view.hpp
    bsoncxx/v_noabi/bsoncxx/stdx/type_traits.hpp
    bsoncxx/v_noabi/bsoncxx/string/to_string.hpp
    bsoncxx/v_noabi/bsoncxx/string/view_or_value-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/string/view_or_value.hpp
    bsoncxx/v_noabi/bsoncxx/types-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/types.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/make_value.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/value-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/value.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/view_or_value.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/view-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/types/bson_value/view.hpp
    bsoncxx/v_noabi/bsoncxx/types/value.hpp
    bsoncxx/v_noabi/bsoncxx/util/functor.hpp
    bsoncxx/v_noabi/bsoncxx/validate-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/validate.hpp
    bsoncxx/v_noabi/bsoncxx/view_or_value-fwd.hpp
    bsoncxx/v_noabi/bsoncxx/view_or_value.hpp
)
