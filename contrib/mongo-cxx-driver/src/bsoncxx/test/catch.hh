// Copyright 2017 MongoDB Inc.
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

#pragma once

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/stdx/operators.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/test/to_string.hh>
#include <third_party/catch/include/catch.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace Catch {
using namespace bsoncxx;

// Catch2 must be able to stringify documents, optionals, etc. if they're used in Catch2 macros.

template <>
struct StringMaker<bsoncxx::oid> {
    static std::string convert(const bsoncxx::oid& value) {
        return value.to_string();
    }
};

template <>
struct StringMaker<bsoncxx::document::view> {
    static std::string convert(const bsoncxx::document::view& value) {
        return bsoncxx::to_json(value, ExtendedJsonMode::k_relaxed);
    }
};

template <>
struct StringMaker<bsoncxx::document::view_or_value> {
    static std::string convert(const bsoncxx::document::view_or_value& value) {
        return StringMaker<bsoncxx::document::view>::convert(value.view());
    }
};

template <>
struct StringMaker<bsoncxx::document::value> {
    static std::string convert(const bsoncxx::document::value& value) {
        return StringMaker<bsoncxx::document::view>::convert(value.view());
    }
};

template <>
struct StringMaker<bsoncxx::types::bson_value::view> {
    static std::string convert(const bsoncxx::types::bson_value::view& value) {
        return '{' + to_string(value.type()) + ": " + to_string(value) + '}';
    }
};

template <>
struct StringMaker<bsoncxx::types::bson_value::value> {
    static std::string convert(const bsoncxx::types::bson_value::value& value) {
        return StringMaker<bsoncxx::types::bson_value::view>::convert(value.view());
    }
};

template <>
struct StringMaker<bsoncxx::types::bson_value::view_or_value> {
    static std::string convert(const bsoncxx::types::bson_value::view_or_value& value) {
        return StringMaker<bsoncxx::types::bson_value::view>::convert(value.view());
    }
};

template <typename T>
struct StringMaker<stdx::optional<T>> {
    static std::string convert(const bsoncxx::stdx::optional<T>& value) {
        if (value) {
            return StringMaker<T>::convert(value.value());
        }

        return "{nullopt}";
    }
};

template <>
struct StringMaker<stdx::optional<bsoncxx::stdx::nullopt_t>> {
    static std::string convert(const bsoncxx::stdx::optional<bsoncxx::stdx::nullopt_t>&) {
        return "{nullopt}";
    }
};

template <>
struct StringMaker<bsoncxx::detail::strong_ordering> {
    static std::string convert(bsoncxx::detail::strong_ordering o) {
        if (o < 0) {
            return "[less-than]";
        } else if (o > 0) {
            return "[greater-than]";
        } else {
            return "[equal/equivalent]";
        }
    }
};

}  // namespace Catch

#include <bsoncxx/config/private/postlude.hh>
