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

#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/private/libbson.hh>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {

oid::oid() {
    bson_oid_t oid;
    bson_oid_init(&oid, nullptr);

    std::memcpy(_bytes.data(), oid.bytes, sizeof(oid.bytes));
}

oid::oid(const stdx::string_view& str) {
    if (!bson_oid_is_valid(str.data(), str.size())) {
        throw bsoncxx::v_noabi::exception{error_code::k_invalid_oid};
    }
    bson_oid_t oid;
    bson_oid_init_from_string(&oid, str.data());
    memcpy(_bytes.data(), oid.bytes, _bytes.size());
}

oid::oid(const char* bytes, std::size_t len) {
    if (len != this->size()) {
        throw bsoncxx::v_noabi::exception{error_code::k_invalid_oid};
    }
    std::memcpy(_bytes.data(), bytes, _bytes.size());
}

std::string oid::to_string() const {
    bson_oid_t oid;
    std::memcpy(oid.bytes, _bytes.data(), sizeof(oid.bytes));
    char str[25];

    bson_oid_to_string(&oid, str);

    return std::string(str);
}

std::time_t oid::get_time_t() const {
    bson_oid_t oid;
    std::memcpy(oid.bytes, _bytes.data(), sizeof(oid.bytes));

    return bson_oid_get_time_t(&oid);
}

const char* oid::bytes() const {
    return _bytes.data();
}

int oid_compare(const oid& lhs, const oid& rhs) {
    bson_oid_t lhs_oid;
    bson_oid_t rhs_oid;

    std::memcpy(lhs_oid.bytes, lhs.bytes(), sizeof(lhs_oid.bytes));
    std::memcpy(rhs_oid.bytes, rhs.bytes(), sizeof(rhs_oid.bytes));

    return bson_oid_compare(&lhs_oid, &rhs_oid);
}

bool BSONCXX_CALL operator<(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) < 0;
}

bool BSONCXX_CALL operator>(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) > 0;
}

bool BSONCXX_CALL operator<=(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) <= 0;
}

bool BSONCXX_CALL operator>=(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) >= 0;
}

bool BSONCXX_CALL operator==(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) == 0;
}

bool BSONCXX_CALL operator!=(const oid& lhs, const oid& rhs) {
    return oid_compare(lhs, rhs) != 0;
}

}  // namespace v_noabi
}  // namespace bsoncxx
