// Copyright 2020-present MongoDB Inc.
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

#include "assert.hh"

#include <iomanip>
#include <numeric>
#include <sstream>
#include <string>

#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/to_string.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <third_party/catch/include/catch.hpp>

using namespace bsoncxx;
using namespace mongocxx;

using bsoncxx::types::bson_value::value;

namespace {

/// When we are matching two documents, we walk down them recursively. To aid test debugging, store
/// a path as we go, in this variable:
thread_local std::vector<std::string> S_match_doc_path;

/**
 * @brief Declare that the current scope is stepping into the given string key of a key-value
 * mapping
 */
struct match_scope_doc_key {
    match_scope_doc_key(bsoncxx::stdx::string_view key) {
        S_match_doc_path.push_back("/" + std::string(key));
    }

    ~match_scope_doc_key() {
        S_match_doc_path.pop_back();
    }
};

/**
 * @brief Declare that the current scope is stepping into the given index of an array
 */
struct match_scope_array_idx {
    match_scope_array_idx(int idx) {
        S_match_doc_path.push_back("[" + std::to_string(idx) + "]");
    }

    ~match_scope_array_idx() {
        S_match_doc_path.pop_back();
    }
};

/**
 * @brief Obtain a string representation of the current path in the matched object
 */
std::string match_doc_current_path() noexcept {
    return std::accumulate(S_match_doc_path.cbegin(), S_match_doc_path.cend(), std::string());
}

}  // namespace

template <typename Element>
type to_type(const Element& type) {
    auto type_str = string::to_string(type.get_string().value);
    if (type_str == "binData")
        return bsoncxx::type::k_binary;
    if (type_str == "long")
        return bsoncxx::type::k_int64;
    if (type_str == "int")
        return bsoncxx::type::k_int32;
    if (type_str == "double")
        return bsoncxx::type::k_double;
    if (type_str == "objectId")
        return bsoncxx::type::k_oid;
    if (type_str == "object")
        return bsoncxx::type::k_document;
    if (type_str == "date")
        return bsoncxx::type::k_date;
    if (type_str == "string")
        return bsoncxx::type::k_string;
    if (type_str == "array")
        return bsoncxx::type::k_array;
    throw std::logic_error{"unrecognized element type '" + type_str + "'"};
}

bool is_set(types::bson_value::view val) {
    return val.type() != bsoncxx::type::k_null;
}

void special_operator(types::bson_value::view actual,
                      document::view expected,
                      entity::map& map,
                      bool is_root) {
    auto op = *expected.begin();
    REQUIRE(string::to_string(op.key()).rfind("$$", 0) == 0);  // assert special operator

    CAPTURE(match_doc_current_path());
    if (string::to_string(op.key()) == "$$type") {
        auto actual_t = actual.type();
        if (op.type() == type::k_string) {
            REQUIRE(actual_t == to_type(op));
            return;
        }

        REQUIRE(op.type() == type::k_array);
        for (auto t : op.get_array().value) {
            if (actual_t == to_type(t)) {
                return;
            }
        }

        FAIL("type '" + to_string(actual_t) + "' expected in array '" +
             to_json(op.get_array().value) + "'");
    } else if (string::to_string(op.key()) == "$$unsetOrMatches") {
        auto val = op.get_value();
        if (is_set(actual)) {
            // $$unsetOrMatches as root document should treat match as root document.
            assert::matches(actual, val, map, is_root);
        }
    } else if (string::to_string(op.key()) == "$$sessionLsid") {
        auto id = string::to_string(op.get_string().value);
        const auto& type = map.type(id);
        if (type == typeid(client_session)) {
            REQUIRE(actual == map.get_client_session(id).id());
        } else {
            // If the map does not contain the client session, then its ID should have been cached
            // as a BSON value.
            REQUIRE(type == typeid(value));
            REQUIRE(actual == map.get_value(id));
        }
    } else if (string::to_string(op.key()) == "$$matchesEntity") {
        auto name = string::to_string(op.get_string().value);
        REQUIRE(actual == map.get_value(name));
    } else if (string::to_string(op.key()) == "$$exists") {
        const bool should_exist = op.get_bool();
        const bool does_exist = actual.type() != bsoncxx::type::k_null;
        if (does_exist && !should_exist) {
            FAIL_CHECK("Expected this document element to be absent, but it is present");
        } else if (!does_exist && should_exist) {
            FAIL_CHECK("Expected this document element to be present, but it is absent");
        }
    } else if (string::to_string(op.key()) == "$$matchesHexBytes") {
        REQUIRE(actual.type() == bsoncxx::type::k_binary);

        auto expected_bytes = test_util::convert_hex_string_to_bytes(op.get_value().get_string());
        decltype(expected_bytes) actual_bytes(actual.get_binary().bytes, actual.get_binary().size);

        REQUIRE(actual_bytes == expected_bytes);
    } else {
        FAIL("unrecognized special operator '" + string::to_string(op.key()) + "'");
    }
}

template <typename T>
bool is_special(T doc) {
    return doc.type() == type::k_document && test_util::size(doc.get_document().value) == 1 &&
           string::to_string(doc.get_document().value.begin()->key()).rfind("$$", 0) == 0;
}

void matches_document(types::bson_value::view actual,
                      types::bson_value::view expected,
                      entity::map& map,
                      bool is_root) {
    if (is_special(expected)) {
        special_operator(actual, expected.get_document(), map, is_root);
        return;
    }

    REQUIRE(actual.type() == bsoncxx::type::k_document);
    const auto actual_doc = actual.get_document().value;
    const auto expected_doc = expected.get_document().value;
    auto extra_fields = test_util::size(actual_doc);

    CAPTURE(to_json(expected_doc));
    CAPTURE(to_json(actual_doc));

    for (auto&& kvp : expected_doc) {
        match_scope_doc_key scope_key{string::to_string(kvp.key())};
        if (is_special(kvp)) {
            if (!actual_doc[kvp.key()]) {
                special_operator(value(nullptr), kvp.get_document(), map, false);
                continue;
            }
        }

        if (!actual_doc[kvp.key()]) {
            FAIL("expected field '" + string::to_string(kvp.key()) +
                 "' to be present, but it is absent");
        }

        assert::matches(actual_doc[kvp.key()].get_value(), kvp.get_value(), map, false);
        --extra_fields;
    }

    if (!is_root && extra_fields > 0) {
        FAIL("match failed: non-root document contains " + std::to_string(extra_fields) +
             " extra fields");
    }
}

void matches_array(types::bson_value::view actual,
                   types::bson_value::view expected,
                   entity::map& map,
                   bool is_array_of_root_docs = false) {
    REQUIRE(actual.type() == bsoncxx::type::k_array);

    const auto actual_arr = actual.get_array().value;
    const auto expected_arr = expected.get_array().value;

    CAPTURE(to_json(expected_arr));
    CAPTURE(to_json(actual_arr));

    REQUIRE(test_util::size(actual_arr) == test_util::size(expected_arr));
    int idx = 0;
    for (auto a = actual_arr.begin(), e = expected_arr.begin(); e != expected_arr.end();
         e++, a++, ++idx) {
        match_scope_array_idx scope_idx{idx};
        assert::matches(a->get_value(), e->get_value(), map, is_array_of_root_docs);
    }
}

void assert::matches(types::bson_value::view actual,
                     types::bson_value::view expected,
                     entity::map& map,
                     bool is_root,
                     bool is_array_of_root_docs) {
    switch (expected.type()) {
        case bsoncxx::type::k_document:
            matches_document(actual, expected, map, is_root);
            return;
        case bsoncxx::type::k_array:
            matches_array(actual, expected, map, is_array_of_root_docs);
            return;
        case bsoncxx::type::k_int32:
        case bsoncxx::type::k_int64:
        case bsoncxx::type::k_double: {
            CAPTURE(is_root,
                    to_string(actual.type()),
                    to_string(expected.type()),
                    to_string(actual),
                    to_string(expected),
                    match_doc_current_path());
            REQUIRE(test_util::is_numeric(actual));
            REQUIRE(test_util::as_double(expected) == test_util::as_double(actual));
            return;
        }
        default: {
            CAPTURE(is_root,
                    to_string(actual.type()),
                    to_string(expected.type()),
                    to_string(actual),
                    to_string(expected),
                    match_doc_current_path());
            REQUIRE(actual.type() == expected.type());
            REQUIRE(actual == expected);
        }
    }
}
