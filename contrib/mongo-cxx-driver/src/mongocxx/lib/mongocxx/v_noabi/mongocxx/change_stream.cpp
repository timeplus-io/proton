// Copyright 2018-present MongoDB Inc.
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

#include <string>

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/private/change_stream.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

// Requirements for concept Iterator:
// https://en.cppreference.com/w/cpp/named_req/Iterator
static_assert(std::is_copy_constructible<change_stream::iterator>::value, "");
static_assert(std::is_copy_assignable<change_stream::iterator>::value, "");
static_assert(std::is_destructible<change_stream::iterator>::value, "");

// Below basically assert that we have the traits on change_stream::iterator
// so they can't be accidentally removed.
static_assert(std::is_integral<change_stream::iterator::difference_type>::value, "");
static_assert(std::is_class<change_stream::iterator::value_type>::value, "");
static_assert(std::is_pointer<change_stream::iterator::pointer>::value, "");
static_assert(std::is_reference<change_stream::iterator::reference>::value, "");

change_stream::change_stream(change_stream&&) noexcept = default;

change_stream& change_stream::operator=(change_stream&&) noexcept = default;

change_stream::~change_stream() = default;

change_stream::iterator change_stream::begin() const {
    if (_impl->is_dead()) {
        return end();
    }
    return iterator{change_stream::iterator::iter_type::k_tracking, this};
}

change_stream::iterator change_stream::end() const {
    return iterator{change_stream::iterator::iter_type::k_end, this};
}

stdx::optional<bsoncxx::v_noabi::document::view> change_stream::get_resume_token() const {
    return _impl->get_resume_token();
}

// void* since we don't leak C driver defs into C++ driver
change_stream::change_stream(void* change_stream_ptr)
    : _impl(stdx::make_unique<impl>(static_cast<mongoc_change_stream_t*>(change_stream_ptr))) {}

change_stream::iterator::iterator()
    : change_stream::iterator::iterator{iter_type::k_default_constructed, nullptr} {}

const bsoncxx::v_noabi::document::view& change_stream::iterator::operator*() const {
    return _change_stream->_impl->doc();
}

const bsoncxx::v_noabi::document::view* change_stream::iterator::operator->() const {
    return std::addressof(_change_stream->_impl->doc());
}

change_stream::iterator& change_stream::iterator::operator++() {
    if (_type == iter_type::k_tracking) {
        _change_stream->_impl->advance_iterator();
    }
    return *this;
}

void change_stream::iterator::operator++(int) {
    operator++();
}

change_stream::iterator::iterator(const iter_type type, const change_stream* change_stream)
    : _type{type}, _change_stream{change_stream} {
    if (type != iter_type::k_tracking || _change_stream->_impl->has_started()) {
        return;
    }

    _change_stream->_impl->mark_started();
    // Advance to first event on begin() to keep operator*() state-machine-free.
    operator++();
}

// Care about the underlying change_stream being the same so we can
// support a collection of iterators for change streams from different
// collections.
bool MONGOCXX_CALL operator==(const change_stream::iterator& lhs,
                              const change_stream::iterator& rhs) noexcept {
    // Tracking different streams never equal.
    if (lhs._change_stream != rhs._change_stream) {
        return false;
    }
    // User-constructed:
    if (rhs._change_stream == nullptr) {
        return lhs._change_stream == nullptr;
    }
    // Both end or tracking:
    if (rhs._type == lhs._type) {
        return rhs.is_exhausted() == lhs.is_exhausted();
    }
    return
        // Either one side is .end()-constructed, and the other is exhausted
        ((lhs._type == change_stream::iterator::iter_type::k_end) && rhs.is_exhausted()) ||
        ((rhs._type == change_stream::iterator::iter_type::k_end) && lhs.is_exhausted());
}

bool MONGOCXX_CALL operator!=(const change_stream::iterator& lhs,
                              const change_stream::iterator& rhs) noexcept {
    return !(lhs == rhs);
}

bool change_stream::iterator::is_exhausted() const {
    return _change_stream->_impl->is_exhausted();
}

}  // namespace v_noabi
}  // namespace mongocxx
