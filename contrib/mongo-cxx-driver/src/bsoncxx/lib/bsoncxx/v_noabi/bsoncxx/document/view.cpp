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

#include <cstring>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/types.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {
namespace document {

namespace {

bson_iter_t to_bson_iter_t(element e) {
    bson_iter_t iter{};
    bson_iter_init_from_data_at_offset(&iter, e.raw(), e.length(), e.offset(), e.keylen());
    return iter;
}

}  // namespace

view::const_iterator::const_iterator() {}

view::const_iterator::const_iterator(const element& element) : _element(element) {}

view::const_iterator::reference view::const_iterator::operator*() {
    return _element;
}

view::const_iterator::pointer view::const_iterator::operator->() {
    return &_element;
}

view::const_iterator& view::const_iterator::operator++() {
    if (!_element) {
        return *this;
    }

    // the bson_t pointer and length remain unchanged while iterating.
    auto raw = _element.raw();
    auto len = _element.length();

    bson_iter_t iter = to_bson_iter_t(_element);

    if (!bson_iter_next(&iter)) {
        _element = element{};
    } else {
        _element = element{raw, len, bson_iter_offset(&iter), bson_iter_key_len(&iter)};
    }

    return *this;
}

view::const_iterator view::const_iterator::operator++(int) {
    const_iterator before(*this);
    operator++();
    return before;
}

bool BSONCXX_CALL operator==(const view::const_iterator& lhs, const view::const_iterator& rhs) {
    return std::forward_as_tuple(lhs._element.raw(), lhs._element.offset()) ==
           std::forward_as_tuple(rhs._element.raw(), rhs._element.offset());
}

bool BSONCXX_CALL operator!=(const view::const_iterator& lhs, const view::const_iterator& rhs) {
    return !(lhs == rhs);
}

view::const_iterator view::cbegin() const {
    bson_iter_t iter;

    if (!bson_iter_init_from_data(&iter, data(), length())) {
        return cend();
    }

    if (!bson_iter_next(&iter)) {
        return cend();
    }

    return const_iterator{element{data(),
                                  static_cast<uint32_t>(length()),
                                  bson_iter_offset(&iter),
                                  bson_iter_key_len(&iter)}};
}

view::const_iterator view::cend() const {
    return const_iterator{};
}

view::const_iterator view::begin() const {
    return cbegin();
}

view::const_iterator view::end() const {
    return cend();
}

view::const_iterator view::find(stdx::string_view key) const {
    bson_t b;
    if (!bson_init_static(&b, _data, _length)) {
        // return invalid element with key to provide more helpful exception message.
        return const_iterator(element(key));
    }

    bson_iter_t iter;

    // Logically, a default constructed string_view represents the
    // empty string just as does string_view(""), but they have,
    // potentially, different represntations, the former having .data
    // returning nullptr though the latter probably does not. But the
    // C functions like strncmp below can't be called with nullptr. If
    // we were called with a string_data such that its .data() member
    // returns nullptr, then, barring undefined behavior, its length
    // is known to be zero, and it is equivalent to the empty string,
    // an instance of which we reset it to.
    if (key.data() == nullptr) {
        key = "";
    }

    if (!bson_iter_init_find_w_len(&iter, &b, key.data(), static_cast<int>(key.size()))) {
        // returning `cend()` returns an element without a key or value.
        // return invalid element with key to provide more helpful exception
        return const_iterator(element(key));
    }

    return const_iterator(element(
        _data, static_cast<uint32_t>(_length), bson_iter_offset(&iter), bson_iter_key_len(&iter)));
}

element view::operator[](stdx::string_view key) const {
    return *(this->find(key));
}

view::view(const std::uint8_t* data, std::size_t length) : _data(data), _length(length) {}

namespace {
const uint8_t k_default_view[5] = {5, 0, 0, 0, 0};
}

view::view() : _data(k_default_view), _length(sizeof(k_default_view)) {}

const std::uint8_t* view::data() const {
    return _data;
}
std::size_t view::length() const {
    return _length;
}

bool view::empty() const {
    return _length == 5;
}

bool BSONCXX_CALL operator==(view lhs, view rhs) {
    return (lhs.length() == rhs.length()) &&
           (std::memcmp(lhs.data(), rhs.data(), lhs.length()) == 0);
}

bool BSONCXX_CALL operator!=(view lhs, view rhs) {
    return !(lhs == rhs);
}

}  // namespace document
}  // namespace v_noabi
}  // namespace bsoncxx
