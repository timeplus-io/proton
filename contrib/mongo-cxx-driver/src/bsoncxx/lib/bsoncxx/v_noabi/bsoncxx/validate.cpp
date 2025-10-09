// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/validate.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {

struct validator::impl {
    bool _check_utf8{false};
    bool _check_utf8_allow_null{false};
    bool _check_dollar_keys{false};
    bool _check_dot_keys{false};
};

validator::validator() : _impl{stdx::make_unique<impl>()} {}

validator::~validator() = default;

void validator::check_utf8(bool check_utf8) {
    _impl->_check_utf8 = check_utf8;
}

bool validator::check_utf8() const {
    return _impl->_check_utf8;
}

void validator::check_utf8_allow_null(bool check_utf8_allow_null) {
    _impl->_check_utf8_allow_null = check_utf8_allow_null;
}

bool validator::check_utf8_allow_null() const {
    return _impl->_check_utf8_allow_null;
}

void validator::check_dollar_keys(bool check_dollar_keys) {
    _impl->_check_dollar_keys = check_dollar_keys;
}

bool validator::check_dollar_keys() const {
    return _impl->_check_dollar_keys;
}

void validator::check_dot_keys(bool check_dot_keys) {
    _impl->_check_dot_keys = check_dot_keys;
}

bool validator::check_dot_keys() const {
    return _impl->_check_dot_keys;
}

stdx::optional<document::view> BSONCXX_CALL validate(const std::uint8_t* data, std::size_t length) {
    const validator vtor{};
    return validate(data, length, vtor);
}

stdx::optional<document::view> BSONCXX_CALL validate(const std::uint8_t* data,
                                                     std::size_t length,
                                                     const validator& validator,
                                                     std::size_t* invalid_offset) {
    ::bson_validate_flags_t flags = BSON_VALIDATE_NONE;

    const auto flip_if = [&flags](bool cond, ::bson_validate_flags_t flag) {
        if (cond) {
            // this static cast needed to get around invalid conversion warnings...
            flags = static_cast<::bson_validate_flags_t>(flags | flag);
        }
    };

    flip_if(validator.check_dot_keys(), BSON_VALIDATE_DOT_KEYS);
    flip_if(validator.check_dollar_keys(), BSON_VALIDATE_DOLLAR_KEYS);
    // we enable VALIDATE_UTF8 if the user wants VALIDATE_UTF8_ALLOW_NULL
    // otherwise validate_utf8_allow_null() would do nothing due to how libbson
    // interprets the flag.
    flip_if((validator.check_utf8() || validator.check_utf8_allow_null()), BSON_VALIDATE_UTF8);
    flip_if(validator.check_utf8_allow_null(), BSON_VALIDATE_UTF8_ALLOW_NULL);

    ::bson_t bson;
    if (!::bson_init_static(&bson, data, length)) {
        // if we can't even initialize a bson_t we just say the error is at offset 0.
        if (invalid_offset)
            *invalid_offset = 0u;
        return {};
    }

    if (!::bson_validate(&bson, flags, invalid_offset)) {
        return {};
    }

    return document::view{data, length};
}

}  // namespace v_noabi
}  // namespace bsoncxx
