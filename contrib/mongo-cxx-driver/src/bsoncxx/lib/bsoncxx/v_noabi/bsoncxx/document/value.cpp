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

#include <utility>

#include <bsoncxx/document/value.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {
namespace document {

value::value(std::uint8_t* data, std::size_t length, deleter_type dtor)
    : _data(data, dtor), _length(length) {}

value::value(unique_ptr_type ptr, std::size_t length) : _data(std::move(ptr)), _length(length) {}

namespace {

void uint8_t_deleter(std::uint8_t* ptr) {
    delete[] ptr;
}

}  // namespace

value::value(document::view view)
    : _data(new std::uint8_t[static_cast<std::size_t>(view.length())], uint8_t_deleter),
      _length(view.length()) {
    std::copy(view.data(), view.data() + view.length(), _data.get());
}

value::value(const value& rhs) : value(rhs.view()) {}

value& value::operator=(const value& rhs) {
    *this = value{rhs.view()};
    return *this;
}

document::view::const_iterator value::cbegin() const {
    return this->view().cbegin();
}

document::view::const_iterator value::cend() const {
    return this->view().cend();
}

document::view::const_iterator value::begin() const {
    return cbegin();
}

document::view::const_iterator value::end() const {
    return cend();
}

document::view::const_iterator value::find(stdx::string_view key) const {
    return this->view().find(key);
}

element value::operator[](stdx::string_view key) const {
    auto view = this->view();
    return view[key];
}

const std::uint8_t* value::data() const {
    return _data.get();
}

std::size_t value::length() const {
    return _length;
}

bool value::empty() const {
    return _length == 5;
}

value::unique_ptr_type value::release() {
    _length = 0;
    return std::move(_data);
}

void value::reset(document::view view) {
    _data.reset(new std::uint8_t[static_cast<std::size_t>(view.length())]);
    _length = view.length();
    std::copy(view.data(), view.data() + view.length(), _data.get());
}

}  // namespace document
}  // namespace v_noabi
}  // namespace bsoncxx
