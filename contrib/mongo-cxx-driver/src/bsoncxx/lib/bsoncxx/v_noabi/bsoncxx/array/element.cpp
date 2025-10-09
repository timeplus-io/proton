// Copyright 2014 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdexcept>

#include <bsoncxx/array/element.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {
namespace array {

element::element() : document::element() {}

element::element(const std::uint8_t* raw,
                 std::uint32_t length,
                 std::uint32_t offset,
                 std::uint32_t keylen)
    : document::element(raw, length, offset, keylen) {}

element::element(const stdx::string_view key) : document::element(key) {}

bool BSONCXX_CALL operator==(const element& elem, const types::bson_value::view& v) {
    return elem.get_value() == v;
}

bool BSONCXX_CALL operator==(const types::bson_value::view& v, const element& elem) {
    return elem == v;
}

bool BSONCXX_CALL operator!=(const element& elem, const types::bson_value::view& v) {
    return !(elem == v);
}

bool BSONCXX_CALL operator!=(const types::bson_value::view& v, const element& elem) {
    return !(elem == v);
}

}  // namespace array
}  // namespace v_noabi
}  // namespace bsoncxx
