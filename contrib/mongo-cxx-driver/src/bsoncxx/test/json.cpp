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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_array.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/test/catch.hh>

namespace {
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

constexpr auto k_invalid_json = R"({])";
constexpr auto k_valid_json = R"({ "a" : 1, "b" : 2.0 })";

TEST_CASE("invalid json throws") {
    using namespace bsoncxx;
    REQUIRE_THROWS_AS(from_json(k_invalid_json), bsoncxx::exception);
}

TEST_CASE("valid json does not throw") {
    using namespace bsoncxx;
    REQUIRE_NOTHROW(from_json(k_valid_json));
}

TEST_CASE("valid json is converted to equivalent BSON") {
    using namespace bsoncxx;

    const auto expected = make_document(kvp("a", 1), kvp("b", 2.0));
    const auto expected_view = expected.view();

    const auto actual_doc = from_json(k_valid_json);
    const auto actual_view = actual_doc.view();

    REQUIRE(expected_view.length() == actual_view.length());
    REQUIRE(0 == memcmp(expected_view.data(), actual_view.data(), expected_view.length()));
}

TEST_CASE("empty document is converted correctly to json string") {
    using namespace bsoncxx;
    REQUIRE(0 == to_json(make_document().view()).compare("{ }"));
}

TEST_CASE("empty array is converted correctly to json string") {
    using bsoncxx::to_json;

    auto doc = make_document(kvp("array", make_array()));
    REQUIRE(0 == to_json(doc.view()).compare(R"({ "array" : [  ] })"));
}

TEST_CASE("CXX-941 is resolved") {
    std::string obj_value = R"({"id1":"val1", "id2":"val2"})";
    auto doc = make_document(kvp("obj_name", obj_value));
    std::string output = bsoncxx::to_json(doc.view());
    REQUIRE(output ==
            "{ \"obj_name\" : \"{\\\"id1\\\":\\\"val1\\\", \\\"id2\\\":\\\"val2\\\"}\" }");
}

TEST_CASE("CXX-1246: Legacy Extended JSON (Implicit)") {
    using namespace bsoncxx;
    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto doc = make_document(kvp("number", 42), kvp("bin", bin_val));
    auto output = bsoncxx::to_json(doc.view());
    REQUIRE(output ==
            R"({ "number" : 42, "bin" : { "$binary" : "ZGVhZGJlZWY=", "$type" : "04" } })");
}

TEST_CASE("CXX-1246: Legacy Extended JSON (Explicit)") {
    using namespace bsoncxx;
    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto doc = make_document(kvp("number", 42), kvp("bin", bin_val));
    auto output = to_json(doc.view(), ExtendedJsonMode::k_legacy);
    REQUIRE(output ==
            R"({ "number" : 42, "bin" : { "$binary" : "ZGVhZGJlZWY=", "$type" : "04" } })");
}

TEST_CASE("CXX-1246: Relaxed Extended JSON") {
    using namespace bsoncxx;
    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto doc = make_document(kvp("number", 42), kvp("bin", bin_val));
    auto output = to_json(doc.view(), ExtendedJsonMode::k_relaxed);

    REQUIRE(
        output ==
        R"({ "number" : 42, "bin" : { "$binary" : { "base64" : "ZGVhZGJlZWY=", "subType" : "04" } } })");
}

TEST_CASE("CXX-1246: Canonical Extended JSON") {
    using namespace bsoncxx;
    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto doc = make_document(kvp("number", 42), kvp("bin", bin_val));
    auto output = to_json(doc.view(), ExtendedJsonMode::k_canonical);

    REQUIRE(
        output ==
        R"({ "number" : { "$numberInt" : "42" }, "bin" : { "$binary" : { "base64" : "ZGVhZGJlZWY=", "subType" : "04" } } })");
}

TEST_CASE("CXX-1712: Overloaded to_json Legacy (Implicit)") {
    using namespace bsoncxx;
    using namespace builder::basic;

    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto arr = make_array(make_document(kvp("foo", 42), kvp("bar", "A"), kvp("baz", bin_val)));
    auto output = bsoncxx::to_json(arr.view());

    REQUIRE(
        output ==
        R"([ { "foo" : 42, "bar" : "A", "baz" : { "$binary" : "ZGVhZGJlZWY=", "$type" : "04" } } ])");
}

TEST_CASE("CXX-1712: Overloaded to_json Legacy (Explicit)") {
    using namespace bsoncxx;
    using namespace builder::basic;

    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto arr = make_array(make_document(kvp("foo", 42), kvp("bar", "A"), kvp("baz", bin_val)));
    auto output = to_json(arr.view(), ExtendedJsonMode::k_legacy);

    REQUIRE(
        output ==
        R"([ { "foo" : 42, "bar" : "A", "baz" : { "$binary" : "ZGVhZGJlZWY=", "$type" : "04" } } ])");
}

TEST_CASE("CXX-1712: Overloaded to_json Relaxed") {
    using namespace bsoncxx;
    using namespace builder::basic;

    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto arr = make_array(make_document(kvp("foo", 42), kvp("bar", "A"), kvp("baz", bin_val)));
    auto output = to_json(arr.view(), ExtendedJsonMode::k_relaxed);

    REQUIRE(
        output ==
        R"([ { "foo" : 42, "bar" : "A", "baz" : { "$binary" : { "base64" : "ZGVhZGJlZWY=", "subType" : "04" } } } ])");
}

TEST_CASE("CXX-1712: Overloaded to_json Canonical") {
    using namespace bsoncxx;
    using namespace builder::basic;

    types::b_binary bin_val{
        binary_sub_type::k_uuid, 8, reinterpret_cast<const uint8_t*>("deadbeef")};
    auto arr = make_array(make_document(kvp("foo", 42), kvp("bar", "A"), kvp("baz", bin_val)));
    auto output = to_json(arr.view(), ExtendedJsonMode::k_canonical);

    REQUIRE(
        output ==
        R"([ { "foo" : { "$numberInt" : "42" }, "bar" : "A", "baz" : { "$binary" : { "base64" : "ZGVhZGJlZWY=", "subType" : "04" } } } ])");
}

TEST_CASE("UDL _bson works like from_json()") {
    using namespace bsoncxx;

    SECTION("_bson and from_json() return the same value") {
        auto expected_value = from_json(k_valid_json);
        auto actual_value = R"({ "a" : 1, "b" : 2.0 })"_bson;

        REQUIRE(actual_value == expected_value);
        REQUIRE("[1, 2, 3]"_bson == from_json("[1, 2, 3]"));
    }

    SECTION("_bson returns an empty document") {
        REQUIRE("{}"_bson == from_json("{  }"));
        REQUIRE("{}"_bson.view().empty());
    }

    SECTION("_bson throws an exception with invalid json") {
        REQUIRE_THROWS_AS(R"({])"_bson, bsoncxx::exception);
        REQUIRE_THROWS_AS(""_bson, bsoncxx::exception);
    }
}
}  // namespace
