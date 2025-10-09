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

#include <chrono>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

namespace {

using namespace bsoncxx;
using namespace types;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::document::element;

TEST_CASE("type to_string", "[bsoncxx::type::to_string]") {
    REQUIRE(to_string(bsoncxx::type::k_bool) == "bool");
}

TEST_CASE("binary_sub_type to_string", "[bsoncxx::type::to_string]") {
    REQUIRE(to_string(bsoncxx::binary_sub_type::k_function) == "function");
}

TEST_CASE("b_double", "[bsoncxx::type::b_double]") {
    b_double a{1.1};
    b_double b{a};
    b_double c{2.1};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_string", "[bsoncxx::type::b_string]") {
    b_string a{"hello"};
    b_string b{a};
    b_string c{"world"};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_document", "[bsoncxx::type::b_document]") {
    auto doc1 = make_document(kvp("a", 1));
    auto doc2 = make_document(kvp("b", 2));
    b_document a{doc1.view()};
    b_document b{a};
    b_document c{doc2.view()};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_array", "[bsoncxx::type::b_array]") {
    builder::basic::array arr;
    arr.append(b_int32{1}, b_int32{2});
    b_array a{arr.view()};
    b_array b{a};
    arr.append(b_int32{3});
    b_array c{arr.view()};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_binary", "[bsoncxx::type::b_binary]") {
    b_binary a{binary_sub_type::k_binary, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    b_binary b{binary_sub_type::k_binary, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    b_binary c{binary_sub_type::k_binary, 8, reinterpret_cast<const uint8_t*>("daedbeef")};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_undefined", "[bsoncxx::type::b_undefined]") {
    b_undefined a;
    REQUIRE(a == a);
}

TEST_CASE("b_oid", "[bsoncxx::type::b_oid]") {
    oid oid1;
    oid oid2;
    b_oid a{bsoncxx::oid{oid1.bytes(), 12}};
    b_oid b{a.value};
    b_oid c{bsoncxx::oid{oid2.bytes(), 12}};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_bool", "[bsoncxx::type::b_undefined]") {
    b_bool a{true};
    b_bool b{a};
    b_bool c{false};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_null", "[bsoncxx::type::b_null]") {
    b_null a;
    REQUIRE(a == a);
}

TEST_CASE("b_regex", "[bsoncxx::type::b_regex]") {
    b_regex a{stdx::string_view{"^foo|bar$"}};
    b_regex b{a.regex};
    b_regex c{stdx::string_view{"^bar|foo$"}};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_code", "[bsoncxx::type::b_code]") {
    b_code a{"var a = {};"};
    b_code b{a};
    b_code c{"var b = 1;"};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_symbol", "[bsoncxx::type::b_symbol]") {
    b_symbol a{"deadbeef"};
    b_symbol b{a};
    b_symbol c{"beefdead"};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_codewscope", "[bsoncxx::type::b_codewscope]") {
    auto scope1 = make_document(kvp("b", 1));
    auto scope2 = make_document(kvp("b", 5));
    b_codewscope a{"var a = b;", scope1.view()};
    b_codewscope b{a.code, a.scope};
    b_codewscope c{"var a = b;", scope2.view()};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_int32", "[bsoncxx::type::b_int32]") {
    b_int32 a{10};
    b_int32 b{a};
    b_int32 c{5};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_timestamp", "[bsoncxx::type::b_timestamp") {
    b_timestamp a{100, 1000};
    b_timestamp b{100, 1000};
    b_timestamp c{200, 1000};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_int64", "[bsoncxx::type::b_int64]") {
    b_int64 a{100};
    b_int64 b{a};
    b_int32 c{200};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_decimal128", "[bsoncxx::type::b_decimal128]") {
    b_decimal128 a{"-1234E+999"};
    b_decimal128 b{a};
    b_decimal128 c{"1234E+999"};
    REQUIRE(a == b);
    REQUIRE(!(a == c));
}

TEST_CASE("b_minkey", "[bsoncxx::type::b_minkey]") {
    b_minkey a;
    REQUIRE(a == a);
}

TEST_CASE("b_maxkey", "[bsoncxx::type::b_maxkey]") {
    b_maxkey a;
    REQUIRE(a == a);
}

TEST_CASE("getting types from an uninitialized element throws", "[bsoncxx::document::element]") {
    element elem{};
    REQUIRE_THROWS(elem.get_value());

#define BSONCXX_ENUM(name, val) REQUIRE_THROWS(elem.get_##name());
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
}

TEST_CASE("bson_value::view returns correct type", "[bsoncxx::types::bson_value::view]") {
    b_bool bool_val{true};
    REQUIRE(bson_value::view{bool_val}.type() == bsoncxx::type::k_bool);
}

TEST_CASE("bson_value::view with b_double", "[bsoncxx::types::bson_value::view]") {
    b_double double_val{1.1};
    REQUIRE(bson_value::view{double_val}.get_double() == double_val);
}

TEST_CASE("bson_value::view with b_string", "[bsoncxx::types::bson_value::view]") {
    b_string string_val{"hello"};
    REQUIRE(bson_value::view{string_val}.get_string() == string_val);
}

TEST_CASE("bson_value::view with b_document", "[bsoncxx::types::bson_value::view]") {
    auto doc = make_document(kvp("a", 1));
    b_document doc_val{doc.view()};
    REQUIRE(bson_value::view{doc_val}.get_document() == doc_val);
}

TEST_CASE("bson_value::view with b_array", "[bsoncxx::types::bson_value::view]") {
    builder::basic::array arr;
    arr.append(b_int32{1}, b_int32{2}, b_int32{3});
    b_array arr_val{arr.view()};
    REQUIRE(bson_value::view{arr_val}.get_array() == arr_val);
}

TEST_CASE("bson_value::view with b_binary", "[bsoncxx::types::bson_value::view]") {
    b_binary binary_val{binary_sub_type::k_binary, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    REQUIRE(bson_value::view{binary_val}.get_binary() == binary_val);
}

TEST_CASE("bson_value::view with b_undefined", "[bsoncxx::types::bson_value::view]") {
    b_undefined undef_val;
    REQUIRE(bson_value::view{undef_val}.get_undefined() == undef_val);
}

TEST_CASE("bson_value::view with b_oid", "[bsoncxx::types::bson_value::view]") {
    oid oid;
    b_oid oid_val{bsoncxx::oid{oid.bytes(), 12}};
    REQUIRE(bson_value::view{oid_val}.get_oid() == oid_val);
}

TEST_CASE("bson_value::view with b_bool", "[bsoncxx::types::bson_value::view]") {
    b_bool bool_val{true};
    REQUIRE(bson_value::view{bool_val}.get_bool() == bool_val);
}

TEST_CASE("bson_value::view with b_date", "[bsoncxx::types::bson_value::view]") {
    using bsoncxx::types::b_date;
    using std::chrono::system_clock;

    b_date date_val{system_clock::now()};
    REQUIRE(bson_value::view{date_val}.get_date() == date_val);
}

TEST_CASE("bson_value::view with b_null", "[bsoncxx::types::bson_value::view]") {
    b_null null_val;
    REQUIRE(bson_value::view{null_val}.get_null() == null_val);
}

TEST_CASE("bson_value::view with b_regex", "[bsoncxx::types::bson_value::view]") {
    b_regex regex_val{stdx::string_view{"^foo|bar$"}};
    REQUIRE(bson_value::view{regex_val}.get_regex() == regex_val);
}

TEST_CASE("bson_value::view with b_code", "[bsoncxx::types::bson_value::view]") {
    b_code code_val{"var a = {};"};
    REQUIRE(bson_value::view{code_val}.get_code() == code_val);
}

TEST_CASE("bson_value::view with b_symbol", "[bsoncxx::types::bson_value::view]") {
    b_symbol symbol_val{"deadbeef"};
    REQUIRE(bson_value::view{symbol_val}.get_symbol() == symbol_val);
}

TEST_CASE("bson_value::view with b_codewscope", "[bsoncxx::types::bson_value::view]") {
    auto scope1 = make_document(kvp("b", 1));
    b_codewscope codewscope_val{"var a = b;", scope1.view()};
    REQUIRE(bson_value::view{codewscope_val}.get_codewscope() == codewscope_val);
}

TEST_CASE("bson_value::view with b_int32", "[bsoncxx::types::bson_value::view]") {
    b_int32 int32_val{10};
    REQUIRE(bson_value::view{int32_val}.get_int32() == int32_val);
}

TEST_CASE("bson_value::view with b_timestamp", "[bsoncxx::types::bson_value::view]") {
    types::b_timestamp timestamp_val{100, 1000};
    REQUIRE(bson_value::view{timestamp_val}.get_timestamp() == timestamp_val);
}

TEST_CASE("bson_value::view with b_int64", "[bsoncxx::types::bson_value::view]") {
    b_int64 int64_val{100};
    REQUIRE(bson_value::view{int64_val}.get_int64() == int64_val);
}

TEST_CASE("bson_value::view with b_decimal128", "[bsoncxx::types::bson_value::view]") {
    b_decimal128 decimal_val{"-1234E+999"};
    REQUIRE(bson_value::view{decimal_val}.get_decimal128() == decimal_val);
}

TEST_CASE("bson_value::view with b_minkey", "[bsoncxx::types::bson_value::view]") {
    b_minkey minkey_val;
    REQUIRE(bson_value::view{minkey_val}.get_minkey() == minkey_val);
}

TEST_CASE("bson_value::view with b_maxkey", "[bsoncxx::types::bson_value::view]") {
    b_maxkey maxkey_val;
    REQUIRE(bson_value::view{maxkey_val}.get_maxkey() == maxkey_val);
}

TEST_CASE("bson_value::view with copy constructor", "[bsoncxx::types::bson_value::view]") {
    b_int32 int32_val{10};
    bson_value::view original{int32_val};
    bson_value::view copy{original};
    REQUIRE(copy.get_int32() == int32_val);
}

TEST_CASE("bson_value::view with assignment operator", "[bsoncxx::types::bson_value::view]") {
    b_int32 int32_val{10};
    bson_value::view original{int32_val};
    bson_value::view copy = original;
    REQUIRE(copy.get_int32() == int32_val);
}

TEST_CASE("bson_value::view with equality operator", "[bsoncxx::types::bson_value::view]") {
    bson_value::view original{b_int32{10}};
    bson_value::view copy{b_int32{10}};
    REQUIRE(original == copy);
}

TEST_CASE("bson_value::view with equality for value and non-value",
          "[bsoncxx::types::bson_value::view]") {
    b_int64 int64_val{100};
    REQUIRE(bson_value::view{int64_val} == int64_val);
}

TEST_CASE("bson_value::view with equality for non-value and value",
          "[bsoncxx::types::bson_value::view]") {
    b_int64 int64_val{100};
    REQUIRE(int64_val == bson_value::view{int64_val});
}

TEST_CASE("bson_value::view with value inequality operator and same type",
          "[bsoncxx::types::bson_value::view]") {
    bson_value::view first{b_int32{10}};
    bson_value::view second{b_int32{5}};
    REQUIRE(first != second);
}

TEST_CASE("bson_value::view with value inequality operator and different type",
          "[bsoncxx::types::bson_value::view]") {
    bson_value::view first{b_int32{10}};
    bson_value::view second{b_int64{10}};
    REQUIRE(first != second);
}

TEST_CASE("bson_value::view with inequality for value and non-value",
          "[bsoncxx::types::bson_value::view]") {
    b_int64 int64_val{100};
    REQUIRE(bson_value::view{b_int64{200}} != int64_val);
}

TEST_CASE("bson_value::view with inequality for non-value and value",
          "[bsoncxx::types::bson_value::view]") {
    b_int64 int64_val{100};
    REQUIRE(int64_val != bson_value::view{b_int64{200}});
}

TEST_CASE("document uninitialized element throws exceptions", "") {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    bsoncxx::document::value doc = make_document(kvp("foo", "bar"));

    REQUIRE_THROWS_WITH(doc["doesnotexist"].get_string().value,
                        Catch::Contains("cannot get string from an uninitialized element with key "
                                        "\"doesnotexist\": unset document::element"));

    REQUIRE_THROWS_WITH(doc["alsodoesnotexist"].get_value(),
                        Catch::Contains("cannot return the type of uninitialized element with key "
                                        "\"alsodoesnotexist\": unset document::element"));

    // Ensure a non-existing element evaluates to false.
    REQUIRE(!doc["doesnotexist"]);
    // Ensure finding a non-existing element results in an end iterator.
    REQUIRE(doc.find("doesnotexist") == doc.cend());
    // Ensure getting a key from a non-existing element results in an exception.
    REQUIRE_THROWS_WITH(
        doc["doesnotexist"].key(),
        Catch::Contains("cannot return the key from an uninitialized element with key "
                        "\"doesnotexist\": unset document::element"));
}

TEST_CASE("array uninitialized element throws exceptions", "") {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_array;
    bsoncxx::array::value arr = make_array("a", "b", "c");

    REQUIRE_THROWS_WITH(arr.view()[3].get_string().value,
                        Catch::Contains("cannot get string from an uninitialized element with key "
                                        "\"3\": unset document::element"));
    // Ensure a non-existing element evaluates to false.
    REQUIRE(!arr.view()[3]);
    // Ensure finding a non-existing element results in an end iterator.
    REQUIRE(arr.view().find(3) == arr.view().cend());
    // Ensure getting a key from a non-existing element results in an exception.
    REQUIRE_THROWS_WITH(
        arr.view()[3].key(),
        Catch::Contains("cannot return the key from an uninitialized element with key "
                        "\"3\": unset document::element"));
}
}  // namespace
