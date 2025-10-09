// Copyright 2016 MongoDB Inc.
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

#include <bsoncxx/decimal128.hpp>
#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {

decimal128::decimal128(stdx::string_view str) {
    bson_decimal128_t d128;
    if (!bson_decimal128_from_string(string::to_string(str).c_str(), &d128)) {
        throw bsoncxx::v_noabi::exception{error_code::k_invalid_decimal128};
    }
    _high = d128.high;
    _low = d128.low;
}

std::string decimal128::to_string() const {
    bson_decimal128_t d128;
    d128.high = _high;
    d128.low = _low;
    char str[BSON_DECIMAL128_STRING];
    bson_decimal128_to_string(&d128, str);
    return {str};
}

bool BSONCXX_CALL operator==(const decimal128& lhs, const decimal128& rhs) {
    return lhs._high == rhs._high && lhs._low == rhs._low;
}

bool BSONCXX_CALL operator!=(const decimal128& lhs, const decimal128& rhs) {
    return !(lhs == rhs);
}

}  // namespace v_noabi
}  // namespace bsoncxx
