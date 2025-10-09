// Copyright 2020 MongoDB Inc.
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

#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/types/bson_value/private/value.hh>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/private/convert.hh>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {
namespace types {
namespace bson_value {

value::value(b_double v) : value(v.value) {}
value::value(double v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_DOUBLE;
    _impl->_value.value.v_double = v;
}

value::value(b_int32 v) : value(v.value) {}
value::value(int32_t v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_INT32;
    _impl->_value.value.v_int32 = v;
}

value::value(b_int64 v) : value(v.value) {}
value::value(int64_t v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_INT64;
    _impl->_value.value.v_int64 = v;
}

value::value(const char* v) : value(stdx::string_view{v}) {}
value::value(std::string v) : value(stdx::string_view{v}) {}
value::value(b_string v) : value(v.value) {}
value::value(stdx::string_view v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_UTF8;
    _impl->_value.value.v_utf8.str = make_copy_for_libbson(v);
    _impl->_value.value.v_utf8.len = (uint32_t)v.size();
}

value::value(b_null) : value(nullptr) {}
value::value(std::nullptr_t) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_NULL;
}

value::value(b_date v) : value(v.value) {}
value::value(std::chrono::milliseconds v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_DATE_TIME;
    _impl->_value.value.v_datetime = v.count();
}

value::value(b_oid v) : value(v.value) {}
value::value(oid v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_OID;
    std::memcpy(_impl->_value.value.v_oid.bytes, v.bytes(), v.k_oid_length);
}

value::value(b_bool v) : value(v.value) {}
value::value(bool v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_BOOL;
    _impl->_value.value.v_bool = v;
}

value::value(b_maxkey) : value(type::k_maxkey) {}
value::value(b_minkey) : value(type::k_minkey) {}
value::value(b_undefined) : value(type::k_undefined) {}
value::value(const type id) : _impl{stdx::make_unique<impl>()} {
    switch (id) {
        case type::k_minkey:
            _impl->_value.value_type = BSON_TYPE_MINKEY;
            break;
        case type::k_maxkey:
            _impl->_value.value_type = BSON_TYPE_MAXKEY;
            break;
        case type::k_undefined:
            _impl->_value.value_type = BSON_TYPE_UNDEFINED;
            break;
        default:
            throw bsoncxx::v_noabi::exception(error_code::k_invalid_bson_type_id);
    }
}

value::value(b_regex v) : value(v.regex, v.options) {}
value::value(stdx::string_view regex, stdx::string_view options)
    : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_REGEX;
    _impl->_value.value.v_regex.regex = make_copy_for_libbson(regex);
    _impl->_value.value.v_regex.options = options.empty() ? NULL : make_copy_for_libbson(options);
}

value::value(b_code v) : value(v.type_id, v) {}
value::value(b_symbol v) : value(v.type_id, v) {}
value::value(const type id, stdx::string_view v) : _impl{stdx::make_unique<impl>()} {
    switch (id) {
        case type::k_regex:
            _impl->_value.value_type = BSON_TYPE_REGEX;
            _impl->_value.value.v_regex.regex = make_copy_for_libbson(v);
            _impl->_value.value.v_regex.options = NULL;
            break;
        case type::k_code:
            _impl->_value.value_type = BSON_TYPE_CODE;
            _impl->_value.value.v_code.code = make_copy_for_libbson(v);
            _impl->_value.value.v_code.code_len = (uint32_t)v.length();
            break;
        case type::k_symbol:
            _impl->_value.value_type = BSON_TYPE_SYMBOL;
            _impl->_value.value.v_symbol.symbol = make_copy_for_libbson(v);
            _impl->_value.value.v_symbol.len = (uint32_t)v.length();
            break;
        default:
            throw bsoncxx::v_noabi::exception(error_code::k_invalid_bson_type_id);
    }
}

value::value(b_decimal128 v) : value(v.value) {}
value::value(decimal128 v) : value(type::k_decimal128, v.high(), v.low()) {}
value::value(b_timestamp v) : value(v.type_id, v.increment, v.timestamp) {}
value::value(type id, uint64_t a, uint64_t b) : _impl{stdx::make_unique<impl>()} {
    switch (id) {
        case type::k_decimal128:
            _impl->_value.value_type = BSON_TYPE_DECIMAL128;
            _impl->_value.value.v_decimal128.high = a;
            _impl->_value.value.v_decimal128.low = b;
            break;
        case type::k_timestamp:
            _impl->_value.value_type = BSON_TYPE_TIMESTAMP;
            _impl->_value.value.v_timestamp.increment = (uint32_t)a;
            _impl->_value.value.v_timestamp.timestamp = (uint32_t)b;
            break;
        default:
            throw bsoncxx::v_noabi::exception(error_code::k_invalid_bson_type_id);
    }
}

value::value(b_dbpointer v) : value(v.collection, v.value) {}
value::value(stdx::string_view collection, oid value) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_DBPOINTER;
    _impl->_value.value.v_dbpointer.collection = make_copy_for_libbson(collection);
    _impl->_value.value.v_dbpointer.collection_len = (uint32_t)collection.length();
    std::memcpy(_impl->_value.value.v_dbpointer.oid.bytes, value.bytes(), value.k_oid_length);
}

value::value(b_codewscope v) : value(v.code, v.scope) {}
value::value(stdx::string_view code, bsoncxx::v_noabi::document::view_or_value scope)
    : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_CODEWSCOPE;
    _impl->_value.value.v_codewscope.code = make_copy_for_libbson(code);
    _impl->_value.value.v_codewscope.code_len = (uint32_t)code.length();
    _impl->_value.value.v_codewscope.scope_len = (uint32_t)scope.view().length();
    _impl->_value.value.v_codewscope.scope_data = (uint8_t*)bson_malloc(scope.view().length());
    std::memcpy(
        _impl->_value.value.v_codewscope.scope_data, scope.view().data(), scope.view().length());
}

value::value(b_binary v) : value(v.bytes, v.size, v.sub_type) {}
value::value(std::vector<unsigned char> v, binary_sub_type sub_type)
    : value(v.data(), v.size(), sub_type) {}
value::value(const uint8_t* data, size_t size, const binary_sub_type sub_type)
    : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_BINARY;
    _impl->_value.value.v_binary.subtype = static_cast<bson_subtype_t>(sub_type);
    _impl->_value.value.v_binary.data_len = (uint32_t)size;
    _impl->_value.value.v_binary.data = (uint8_t*)bson_malloc(size);
    if (size)
        std::memcpy(_impl->_value.value.v_binary.data, data, size);
}

value::value(b_document v) : value(v.view()) {}
value::value(bsoncxx::v_noabi::document::view v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_DOCUMENT;
    _impl->_value.value.v_doc.data_len = (uint32_t)v.length();
    _impl->_value.value.v_doc.data = (uint8_t*)bson_malloc(v.length());
    std::memcpy(_impl->_value.value.v_doc.data, v.data(), v.length());
}

value::value(b_array v) : value(v.value) {}
value::value(bsoncxx::v_noabi::array::view v) : _impl{stdx::make_unique<impl>()} {
    _impl->_value.value_type = BSON_TYPE_ARRAY;
    _impl->_value.value.v_doc.data_len = (uint32_t)v.length();
    _impl->_value.value.v_doc.data = (uint8_t*)bson_malloc(v.length());
    std::memcpy(_impl->_value.value.v_doc.data, v.data(), v.length());
}

value::~value() = default;

value::value(value&&) noexcept = default;

value& value::operator=(value&&) noexcept = default;

value::value(const std::uint8_t* raw,
             std::uint32_t length,
             std::uint32_t offset,
             std::uint32_t keylen) {
    bson_iter_t iter;

    bson_iter_init_from_data_at_offset(&iter, raw, length, offset, keylen);
    auto value = bson_iter_value(&iter);

    _impl = stdx::make_unique<impl>(value);
}

value::value(void* internal_value)
    : _impl(stdx::make_unique<impl>((bson_value_t*)internal_value)) {}

value::value(const value& rhs) : value(&rhs._impl->_value) {}

value::value(const bson_value::view& bson_view) {
    _impl = stdx::make_unique<impl>();
    convert_to_libbson(&_impl->_value, bson_view);
}

value& value::operator=(const value& rhs) {
    *this = value{rhs};
    return *this;
}

bson_value::view value::view() const noexcept {
    return _impl->view();
}

value::operator bson_value::view() const noexcept {
    return view();
}

}  // namespace bson_value
}  // namespace types
}  // namespace v_noabi
}  // namespace bsoncxx
