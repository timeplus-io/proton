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

#include <cstring>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/builder/list.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

namespace {

using namespace bsoncxx;

void bson_eq_stream(const bson_t* bson, const bsoncxx::builder::stream::document& builder) {
    bsoncxx::document::view expected(bson_get_data(bson), bson->len);
    bsoncxx::document::view test(builder.view());

    INFO("expected = " << to_json(expected));
    INFO("builder = " << to_json(test));
    REQUIRE(expected.length() == test.length());
    REQUIRE(std::memcmp(expected.data(), test.data(), expected.length()) == 0);
}

template <typename T, typename U>
void viewable_eq_viewable(const T& stream, const U& basic) {
    bsoncxx::document::view expected(stream.view());
    bsoncxx::document::view test(basic.view());

    INFO("expected = " << to_json(expected));
    INFO("basic = " << to_json(test));
    REQUIRE(expected.length() == test.length());
    REQUIRE(std::memcmp(expected.data(), test.data(), expected.length()) == 0);
}

template <typename T>
void bson_eq_object(const bson_t* bson, const T actual) {
    T expected(bson_get_data(bson), bson->len);

    INFO("expected = " << to_json(expected));
    INFO("actual = " << to_json(actual));
    REQUIRE(expected.length() == actual.length());
    REQUIRE(std::memcmp(expected.data(), actual.data(), expected.length()) == 0);
}

TEST_CASE("builder appends string", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_utf8(&expected, "hello", -1, "world", -1);

    builder::stream::document b;

    SECTION("works with string literals") {
        b << "hello"
          << "world";

        bson_eq_stream(&expected, b);
    }

    SECTION("works with std::string") {
        b << "hello" << std::string{"world"};

        bson_eq_stream(&expected, b);
    }

    SECTION("works with b_string") {
        b << "hello" << types::b_string{"world"};

        bson_eq_stream(&expected, b);
    }

    SECTION("works with const char*") {
        const char* world = "world";
        b << "hello" << world;

        bson_eq_stream(&expected, b);
    }

    SECTION("works with char*") {
        char* world = const_cast<char*>("world");
        b << "hello" << world;

        bson_eq_stream(&expected, b);
    }

    // SECTION("fails to compile with non-char*") {
    //     int world = 10;
    //     b << "hello" << &world;

    //     bson_eq_stream(&expected, b);
    // }

    bson_destroy(&expected);
}

TEST_CASE("builder appends double", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_double(&expected, "foo", -1, 1.1);

    builder::stream::document b;

    SECTION("works with raw float") {
        b << "foo" << 1.1;

        bson_eq_stream(&expected, b);
    }

    SECTION("works with b_double") {
        b << "foo" << types::b_double{1.1};

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends binary", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_binary(&expected, "foo", -1, BSON_SUBTYPE_BINARY, (uint8_t*)"deadbeef", 8);

    builder::stream::document b;

    b << "foo" << types::b_binary{binary_sub_type::k_binary, 8, (uint8_t*)"deadbeef"};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends undefined", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_undefined(&expected, "foo", -1);

    builder::stream::document b;

    b << "foo" << types::b_undefined{};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends oid", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_append_oid(&expected, "foo", -1, &oid);

    builder::stream::document b;

    SECTION("b_oid works") {
        b << "foo" << types::b_oid{bsoncxx::oid{(char*)oid.bytes, 12}};

        bson_eq_stream(&expected, b);
    }

    SECTION("raw oid works") {
        b << "foo" << bsoncxx::oid{(char*)oid.bytes, 12};

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends bool", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    SECTION("b_bool true works") {
        bson_append_bool(&expected, "foo", -1, 1);

        b << "foo" << types::b_bool{true};

        bson_eq_stream(&expected, b);
    }

    SECTION("raw true works") {
        bson_append_bool(&expected, "foo", -1, 1);

        b << "foo" << true;

        bson_eq_stream(&expected, b);
    }

    SECTION("b_bool false works") {
        bson_append_bool(&expected, "foo", -1, 0);

        b << "foo" << types::b_bool{false};

        bson_eq_stream(&expected, b);
    }

    SECTION("raw false works") {
        bson_append_bool(&expected, "foo", -1, 0);

        b << "foo" << false;

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends date time", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_date_time(&expected, "foo", -1, 10000);

    b << "foo" << types::b_date{std::chrono::milliseconds{10000}};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends null", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_null(&expected, "foo", -1);

    b << "foo" << types::b_null{};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends regex", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_regex(&expected, "foo", -1, "^foo|bar$", "i");
    bson_append_regex(&expected, "boo", -1, "^boo|far$", "");
    bson_append_regex(&expected, "bar", -1, "^bar|foo$", "");

    b << "foo" << types::b_regex{"^foo|bar$", "i"};
    b << "boo" << types::b_regex{"^boo|far$", ""};
    b << "bar" << types::b_regex{"^bar|foo$"};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends code", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_code(&expected, "foo", -1, "var a = {};");

    b << "foo" << types::b_code{"var a = {};"};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends symbol", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_symbol(&expected, "foo", -1, "deadbeef", -1);

    b << "foo" << types::b_symbol{"deadbeef"};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends code with scope", "[bsoncxx::builder::stream]") {
    bson_t expected, scope;
    bson_init(&expected);
    builder::stream::document b;
    builder::stream::document scope_builder;

    bson_init(&scope);

    bson_append_int32(&scope, "b", -1, 10);

    scope_builder << "b" << 10;

    bson_append_code_with_scope(&expected, "foo", -1, "var a = b;", &scope);

    b << "foo" << types::b_codewscope{"var a = b;", scope_builder.view()};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends int32", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_int32(&expected, "foo", -1, 100);

    SECTION("raw int32") {
        b << "foo" << 100;

        bson_eq_stream(&expected, b);
    }

    SECTION("b_int32") {
        b << "foo" << types::b_int32{100};

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends timestamp", "[bsoncxx::builder::stream]") {
    builder::stream::document b;
    types::b_timestamp foo{100, 1000};
    b << "foo" << foo;

    bson_t expected;
    bson_init(&expected);
    bson_append_timestamp(&expected, "foo", -1, foo.timestamp, foo.increment);

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends int64", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_int64(&expected, "foo", -1, 100);

    SECTION("raw int64") {
        b << "foo" << std::int64_t(100);

        bson_eq_stream(&expected, b);
    }

    SECTION("b_int64") {
        b << "foo" << types::b_int64{100};

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends decimal128", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    bson_decimal128_t d128;
    bson_decimal128_from_string("-1234E+999", &d128);
    bson_append_decimal128(&expected, "foo", -1, &d128);

    builder::stream::document b;

    SECTION("b_decimal128 works") {
        b << "foo" << types::b_decimal128{"-1234E+999"};

        bson_eq_stream(&expected, b);
    }

    SECTION("raw bsoncxx::decimal128 works") {
        b << "foo" << bsoncxx::decimal128{"-1234E+999"};

        bson_eq_stream(&expected, b);
    }

    SECTION("bsoncxx::types::bson_value::view with decimal128 works") {
        auto d = types::b_decimal128{"-1234E+999"};
        auto v = types::bson_value::view{d};

        REQUIRE(v.get_decimal128() == d);

        b << "foo" << v;

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&expected);
}

TEST_CASE("builder appends minkey", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_minkey(&expected, "foo", -1);

    b << "foo" << types::b_minkey{};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends maxkey", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);
    builder::stream::document b;

    bson_append_maxkey(&expected, "foo", -1);

    b << "foo" << types::b_maxkey{};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends array", "[bsoncxx::builder::stream]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);
    builder::stream::document b;
    builder::stream::array child_builder;

    bson_append_utf8(&child, "0", -1, "baz", -1);
    bson_append_array(&expected, "foo", -1, &child);

    child_builder << "baz";

    b << "foo" << types::b_array{child_builder.view()};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("builder appends document", "[bsoncxx::builder::stream]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);
    builder::stream::document b;
    builder::stream::document child_builder;

    bson_append_utf8(&child, "bar", -1, "baz", -1);
    bson_append_document(&expected, "foo", -1, &child);

    child_builder << "bar"
                  << "baz";

    b << "foo" << types::b_document{child_builder.view()};

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("builder appends inline array", "[bsoncxx::builder::stream]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);
    builder::stream::document b;

    bson_append_utf8(&child, "0", -1, "baz", -1);
    bson_append_array(&expected, "foo", -1, &child);

    b << "foo" << builder::stream::open_array << "baz" << builder::stream::close_array;

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("builder appends inline document", "[bsoncxx::builder::stream]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);
    builder::stream::document b;

    bson_append_utf8(&child, "bar", -1, "baz", -1);
    bson_append_document(&expected, "foo", -1, &child);

    b << "foo" << builder::stream::open_document << "bar"
      << "baz" << builder::stream::close_document;

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("builder appends inline nested", "[bsoncxx::builder::stream]") {
    bson_t expected, foo, bar, third;

    bson_init(&expected);
    bson_init(&foo);
    bson_init(&bar);
    bson_init(&third);

    bson_append_utf8(&third, "hello", -1, "world", -1);
    bson_append_int32(&bar, "0", -1, 1);
    bson_append_int32(&bar, "1", -1, 2);
    bson_append_document(&bar, "2", -1, &third);
    bson_append_array(&foo, "bar", -1, &bar);
    bson_append_document(&expected, "foo", -1, &foo);
    builder::stream::document b;

    {
        using namespace builder::stream;

        b << "foo" << open_document << "bar" << open_array << 1 << 2 << open_document << "hello"
          << "world" << close_document << close_array << close_document;
    }

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
    bson_destroy(&foo);
    bson_destroy(&bar);
    bson_destroy(&third);
}

TEST_CASE("builder appends concatenate", "[bsoncxx::builder::stream]") {
    bson_t expected, child;

    bson_init(&expected);
    bson_init(&child);

    builder::stream::document b;

    SECTION("document context works") {
        bson_append_utf8(&child, "hello", -1, "world", -1);
        bson_append_document(&expected, "foo", -1, &child);

        builder::stream::document child_builder;

        child_builder << "hello"
                      << "world";

        {
            using namespace builder::stream;
            b << "foo" << open_document << concatenate(child_builder.view()) << close_document;
        }

        bson_eq_stream(&expected, b);
    }

    SECTION("array context works") {
        bson_append_utf8(&child, "0", -1, "bar", -1);
        bson_append_utf8(&child, "1", -1, "0", -1);
        bson_append_utf8(&child, "2", -1, "baz", -1);
        bson_append_array(&expected, "foo", -1, &child);

        builder::stream::array child_builder;

        child_builder << "0"
                      << "baz";

        {
            using namespace builder::stream;
            b << "foo" << open_array << "bar" << concatenate(child_builder.view()) << close_array;
        }

        bson_eq_stream(&expected, b);
    }

    bson_destroy(&child);
    bson_destroy(&expected);
}

TEST_CASE("builder appends value", "[bsoncxx::builder::stream]") {
    bson_t expected;
    bson_init(&expected);

    builder::stream::document b;
    builder::stream::document tmp;

    bson_append_int32(&expected, "foo", -1, 999);

    tmp << "foo" << 999;

    b << "foo" << tmp.view()["foo"].get_value();

    bson_eq_stream(&expected, b);

    bson_destroy(&expected);
}

TEST_CASE("builder appends lambdas", "[bsoncxx::builder::stream]") {
    builder::stream::document expected;
    builder::stream::document stream;

    {
        using namespace builder::stream;
        expected << "a"
                 << "single"
                 << "b" << open_document << "key1"
                 << "value1"
                 << "key2"
                 << "value2" << close_document << "c" << open_array << 1 << 2 << 3 << close_array;

        stream << "a" << [](single_context s) { s << "single"; } << "b" << open_document <<
            [](key_context<> k) {
                k << "key1"
                  << "value1"
                  << "key2"
                  << "value2";
            } << close_document
               << "c" << open_array << [](array_context<> a) { a << 1 << 2 << 3; } << close_array;
    }

    viewable_eq_viewable(expected, stream);
}

TEST_CASE("document builder finalizes", "[bsoncxx::builder::stream]") {
    builder::stream::document expected;

    expected << "foo" << 999;

    document::value value = builder::stream::document{} << "foo" << 999
                                                        << bsoncxx::builder::stream::finalize;

    viewable_eq_viewable(expected, value);
}

TEST_CASE("array builder finalizes", "[bsoncxx::builder::stream]") {
    builder::stream::array expected;

    expected << 1 << 2 << 3;

    array::value value = builder::stream::array{} << 1 << 2 << 3
                                                  << bsoncxx::builder::stream::finalize;

    viewable_eq_viewable(expected, value);
}

TEST_CASE("document core builder ownership", "[bsoncxx::builder::core]") {
    builder::core b(false);

    SECTION("when passing a std::string key, ownership transfers to the instance") {
        {
            std::string key{"falafel"};
            b.key_owned(key);
        }
        b.append(types::b_int32{1});
        auto doc = b.view_document();
        auto ele = doc["falafel"];
        REQUIRE(ele.type() == type::k_int32);
        REQUIRE(ele.get_value() == types::b_int32{1});
    }

    SECTION("when passing a stdx::string_view, ownership handled by caller") {
        std::string key{"sabich"};
        stdx::string_view key_view(key);
        b.key_view(key_view);
        b.append(1);
    }
}

TEST_CASE("document core builder throws on insufficient stack", "[bsoncxx::builder::core]") {
    builder::core b(false);

    b.key_view("hi");
    REQUIRE_THROWS(b.close_document());
}

TEST_CASE("array core builder throws on insufficient stack", "[bsoncxx::builder::core]") {
    builder::core b(true);

    REQUIRE_THROWS(b.close_array());
}

TEST_CASE("core builder open/close works", "[bsoncxx::builder::core]") {
    builder::core b(false);

    b.key_view("hi");

    SECTION("opening and closing a document works") {
        b.open_document();
        b.close_document();
    }

    SECTION("opening and closing an array works") {
        b.open_array();
        b.close_array();
    }

    SECTION("opening a document and closing an array throws") {
        b.open_document();
        REQUIRE_THROWS(b.close_array());
    }

    SECTION("opening an array and closing a document throws") {
        b.open_array();
        REQUIRE_THROWS(b.close_document());
    }

    SECTION("opening an array and viewing throws") {
        b.open_array();
        REQUIRE_THROWS(b.view_document());
    }

    SECTION("opening a document and viewing throws") {
        b.open_document();
        REQUIRE_THROWS(b.view_document());
    }

    SECTION("viewing with with only the key and no value fails") {
        REQUIRE_THROWS(b.view_document());
    }

    SECTION("viewing with with a key and value suceeds") {
        b.append(10);
        b.view_document();
    }
}

TEST_CASE("core view/extract methods throw when called with wrong top-level type",
          "[bsoncxx::builder::core]") {
    builder::core core_array(true);
    builder::core core_document(false);

    SECTION("view_array only throws when called on document") {
        REQUIRE_NOTHROW(core_array.view_array());
        REQUIRE_THROWS(core_document.view_array());
    }

    SECTION("extract_array only throws when called on document") {
        REQUIRE_NOTHROW(core_array.extract_array());
        REQUIRE_THROWS(core_document.extract_array());
    }

    SECTION("view_document only throws when called on array") {
        REQUIRE_THROWS(core_array.view_document());
        REQUIRE_NOTHROW(core_document.view_document());
    }

    SECTION("extract_document only throws when called on array") {
        REQUIRE_THROWS(core_array.extract_document());
        REQUIRE_NOTHROW(core_document.extract_document());
    }
}

TEST_CASE("core builder throws on consecutive keys", "[bsoncxx::builder::core]") {
    SECTION("appending key_view twice") {
        builder::core builder{false};
        REQUIRE_NOTHROW(builder.key_view("foo"));
        REQUIRE_THROWS_AS(builder.key_view("bar"), bsoncxx::exception);
    }

    SECTION("appending key_view then key_owned") {
        builder::core builder{false};
        REQUIRE_NOTHROW(builder.key_view("foo"));
        REQUIRE_THROWS_AS(builder.key_owned("bar"), bsoncxx::exception);
    }

    SECTION("appending key_owned then key_view") {
        builder::core builder{false};
        REQUIRE_NOTHROW(builder.key_owned("foo"));
        REQUIRE_THROWS_AS(builder.key_view("bar"), bsoncxx::exception);
    }

    SECTION("appending key_owned twice") {
        builder::core builder{false};
        REQUIRE_NOTHROW(builder.key_owned("foo"));
        REQUIRE_THROWS_AS(builder.key_owned("bar"), bsoncxx::exception);
    }
}

TEST_CASE("core method chaining to build document works", "[bsoncxx::builder::core]") {
    auto full_doc = builder::core{false}
                        .key_owned("foo")
                        .append(1)
                        .key_owned("bar")
                        .append(true)
                        .extract_document();

    REQUIRE(full_doc.view()["foo"].type() == types::b_int32::type_id);
    REQUIRE(full_doc.view()["foo"].get_int32() == 1);
    REQUIRE(full_doc.view()["bar"].type() == types::b_bool::type_id);
    REQUIRE(full_doc.view()["bar"].get_bool() == true);
}

TEST_CASE("core method chaining to build array works", "[bsoncxx::builder::core]") {
    auto array = builder::core{true}.append("foo").append(1).append(true).extract_array();
    auto array_view = array.view();

    REQUIRE(std::distance(array_view.begin(), array_view.end()) == 3);
    REQUIRE(array_view[0].type() == type::k_string);
    REQUIRE(string::to_string(array_view[0].get_string().value) == "foo");
    REQUIRE(array_view[1].type() == type::k_int32);
    REQUIRE(array_view[1].get_int32().value == 1);
    REQUIRE(array_view[2].type() == type::k_bool);
    REQUIRE(array_view[2].get_bool().value == true);
}

TEST_CASE("basic document builder works", "[bsoncxx::builder::basic]") {
    builder::stream::document stream;
    builder::basic::document basic;

    stream << "hello"
           << "world";

    SECTION("kvp works") {
        {
            using namespace builder::basic;
            basic.append(kvp("hello", "world"));
        }

        viewable_eq_viewable(stream, basic);
    }

    SECTION("kvp works with std::string key/value") {
        {
            using namespace builder::basic;

            std::string a("hello");
            std::string b("world");
            basic.append(kvp(a, b));
        }

        viewable_eq_viewable(stream, basic);
    }

    SECTION("kvp works with stdx::string_view key/value") {
        {
            using namespace builder::basic;

            stdx::string_view a("hello");
            stdx::string_view b("world");
            basic.append(kvp(a, b));
        }

        viewable_eq_viewable(stream, basic);
    }
    SECTION("variadic works") {
        {
            using namespace builder::stream;
            stream << "foo" << 35 << "bar" << open_document << "que"
                   << "qux" << close_document << "baz" << open_array << 1 << 2 << 3 << close_array;
        }

        {
            using namespace builder::basic;

            basic.append(kvp("hello", "world"),
                         kvp("foo", 35),
                         kvp("bar", [](sub_document sd) { sd.append(kvp("que", "qux")); }),
                         kvp("baz", [](sub_array sa) { sa.append(1, 2, 3); }));
        }

        viewable_eq_viewable(stream, basic);
    }
}

TEST_CASE("basic document builder move semantics work", "[bsoncxx::builder::basic::document]") {
    using builder::basic::kvp;

    builder::basic::document doc_base;
    doc_base.append(kvp("a", "A"));
    doc_base.append(kvp("b", "B"));
    doc_base.append(kvp("bool", true));

    SECTION("move constructor from the same scope") {
        builder::basic::document doc;
        doc.append(kvp("a", "A"));
        doc.append(kvp("b", "B"));
        doc.append(kvp("bool", true));

        builder::basic::document doc_same_scope(std::move(doc));
        viewable_eq_viewable(doc_same_scope, doc_base);

        doc_same_scope.append(kvp("after", true));
        doc_base.append(kvp("after", true));
        viewable_eq_viewable(doc_same_scope, doc_base);
    }

    SECTION("move constructor from outer scope") {
        builder::basic::document doc;
        doc.append(kvp("a", "A"));
        doc.append(kvp("b", "B"));
        doc.append(kvp("bool", true));

        {
            builder::basic::document doc_different_scope(std::move(doc));
            viewable_eq_viewable(doc_different_scope, doc_base);

            doc_different_scope.append(kvp("ds", 12));
            doc_base.append(kvp("ds", 12));
            viewable_eq_viewable(doc_different_scope, doc_base);
        }
    }

    SECTION("move assignment operator from the same scope") {
        builder::basic::document doc;
        doc.append(kvp("a", "A"));
        doc.append(kvp("b", "B"));
        doc.append(kvp("bool", true));
        builder::basic::document doc_from_same_scope = std::move(doc);

        viewable_eq_viewable(doc_from_same_scope, doc_base);

        doc_from_same_scope.append(kvp("next", 12));
        doc_base.append(kvp("next", 12));
        viewable_eq_viewable(doc_from_same_scope, doc_base);
    }

    SECTION("move assignment operator from inner scope") {
        builder::basic::document doc_from_inner_scope;

        {
            builder::basic::document doc;
            doc.append(kvp("a", "A"));
            doc.append(kvp("b", "B"));
            doc.append(kvp("bool", true));
            doc_from_inner_scope = std::move(doc);
        }

        viewable_eq_viewable(doc_from_inner_scope, doc_base);

        doc_from_inner_scope.append(kvp("next", 12));
        doc_base.append(kvp("next", 12));
        viewable_eq_viewable(doc_from_inner_scope, doc_base);
    }

    SECTION("move assignment operator from outer scope") {
        builder::basic::document doc;
        doc.append(kvp("a", "A"));
        doc.append(kvp("b", "B"));
        doc.append(kvp("bool", true));

        {
            builder::basic::document doc_from_outer_scope = std::move(doc);
            viewable_eq_viewable(doc_from_outer_scope, doc_base);

            doc_from_outer_scope.append(kvp("after", true));
            doc_base.append(kvp("after", true));
            viewable_eq_viewable(doc_from_outer_scope, doc_base);
        }
    }

    SECTION("move assignment operator repeated calls") {
        builder::basic::document doc_chain1;
        doc_chain1.append(kvp("a", "A"));
        doc_chain1.append(kvp("b", "B"));
        doc_chain1.append(kvp("bool", true));

        builder::basic::document doc_chain2 = std::move(doc_chain1);
        builder::basic::document doc_chain3 = std::move(doc_chain2);
        viewable_eq_viewable(doc_chain3, doc_base);
    }
}

TEST_CASE("basic array builder move semantics work", "[bsoncxx::builder::basic::array]") {
    builder::basic::array arr_base;
    arr_base.append("a");
    arr_base.append(false);

    SECTION("move constructor from same scope") {
        builder::basic::array arr;
        arr.append("a");
        arr.append(false);

        builder::basic::array arr_from_same_scope(std::move(arr));
        viewable_eq_viewable(arr_from_same_scope, arr_base);

        arr_base.append(12);
        arr_from_same_scope.append(12);
        viewable_eq_viewable(arr_from_same_scope, arr_base);
    }

    SECTION("move constructor from outer scope") {
        builder::basic::array arr;
        arr.append("a");
        arr.append(false);

        {
            builder::basic::array arr_from_outer_scope(std::move(arr));
            viewable_eq_viewable(arr_from_outer_scope, arr_base);

            arr_from_outer_scope.append("after");
            arr_base.append("after");
            viewable_eq_viewable(arr_from_outer_scope, arr_base);
        }
    }

    SECTION("move assignment operator from same scope") {
        builder::basic::array arr;
        arr.append("a");
        arr.append(false);

        builder::basic::array arr_from_same_scope = std::move(arr);
        viewable_eq_viewable(arr_from_same_scope, arr_base);

        arr_base.append(12);
        arr_from_same_scope.append(12);
        viewable_eq_viewable(arr_from_same_scope, arr_base);
    }

    SECTION("move assignment operator from inner scope") {
        builder::basic::array arr_from_inner_scope;

        {
            builder::basic::array arr;
            arr.append("a");
            arr.append(false);
            arr_from_inner_scope = std::move(arr);
        }

        viewable_eq_viewable(arr_from_inner_scope, arr_base);

        arr_from_inner_scope.append(12);
        arr_base.append(12);
        viewable_eq_viewable(arr_from_inner_scope, arr_base);
    }

    SECTION("move assignment operator from outer scope") {
        builder::basic::array arr;
        arr.append("a");
        arr.append(false);

        {
            builder::basic::array arr_from_outer_scope = std::move(arr);
            viewable_eq_viewable(arr_from_outer_scope, arr_base);

            arr_from_outer_scope.append("after");
            arr_base.append("after");
            viewable_eq_viewable(arr_from_outer_scope, arr_base);
        }
    }

    SECTION("move assignment operator repeated calls") {
        builder::basic::array arr;
        arr.append("a");
        arr.append(false);

        builder::basic::array arr_chain_1 = std::move(arr);
        builder::basic::array arr_chain_2 = std::move(arr_chain_1);
        viewable_eq_viewable(arr_chain_2, arr_base);

        arr_chain_2.append(12);
        arr_base.append(12);
        viewable_eq_viewable(arr_chain_2, arr_base);
    }
}

TEST_CASE("basic array builder works", "[bsoncxx::builder::basic]") {
    using namespace builder::basic;

    builder::stream::array stream;
    builder::basic::array basic;

    stream << "hello";

    SECTION("single insert works") {
        basic.append("hello");

        viewable_eq_viewable(stream, basic);
    }

    SECTION("variadic works") {
        stream << 35;

        basic.append("hello", 35);

        viewable_eq_viewable(stream, basic);
    }
}

TEST_CASE("basic document builder works with concat", "[bsoncxx::builder::basic]") {
    using namespace builder::basic;

    auto subdoc = builder::stream::document{} << "hello"
                                              << "world" << builder::stream::finalize;

    builder::stream::document stream;
    builder::basic::document basic;

    stream << builder::stream::concatenate(subdoc.view());

    SECTION("single insert works") {
        basic.append(builder::basic::concatenate(subdoc.view()));

        viewable_eq_viewable(stream, basic);
    }

    SECTION("variadic works") {
        stream << builder::stream::concatenate(subdoc.view());

        basic.append(builder::basic::concatenate(subdoc.view()),
                     builder::basic::concatenate(subdoc.view()));

        viewable_eq_viewable(stream, basic);
    }
}

TEST_CASE("basic array builder works with concat", "[bsoncxx::builder::basic]") {
    using namespace builder::basic;

    auto array_builder = builder::stream::array{} << 1 << 2 << builder::stream::finalize;
    auto array_view = array_builder.view();

    builder::stream::array stream;
    builder::basic::array basic;

    stream << builder::stream::concatenate(array_view);

    SECTION("single insert works") {
        basic.append(builder::basic::concatenate(array_view));

        viewable_eq_viewable(stream, basic);
    }

    SECTION("variadic works") {
        stream << builder::stream::concatenate(array_view);

        basic.append(builder::basic::concatenate(array_view),
                     builder::basic::concatenate(array_view));

        viewable_eq_viewable(stream, basic);
    }
}

TEST_CASE("element throws on bad get_", "[bsoncxx::builder::basic]") {
    using namespace builder::basic;

    builder::stream::array stream;
    builder::basic::array basic;

    stream << "hello";

    SECTION("single insert works") {
        basic.append("hello");

        viewable_eq_viewable(stream, basic);
    }

    SECTION("variadic works") {
        stream << 35;

        basic.append("hello", 35);

        viewable_eq_viewable(stream, basic);
    }
}

TEST_CASE("array::view works", "[bsoncxx::builder::array]") {
    using namespace builder::stream;

    builder::stream::array stream;

    stream << 100 << 99 << 98;

    REQUIRE(stream.view()[0].get_int32() == 100);
    REQUIRE(stream.view()[1].get_int32() == 99);
    REQUIRE(stream.view()[2].get_int32() == 98);
}

TEST_CASE("builder::basic::make_document works", "[bsoncxx::builder::basic::make_document]") {
    auto full_doc = builder::basic::make_document(builder::basic::kvp("foo", 1),
                                                  builder::basic::kvp("bar", true));

    REQUIRE(full_doc.view()["foo"].type() == types::b_int32::type_id);
    REQUIRE(full_doc.view()["foo"].get_int32() == 1);
    REQUIRE(full_doc.view()["bar"].type() == types::b_bool::type_id);
    REQUIRE(full_doc.view()["bar"].get_bool() == true);
}

TEST_CASE("builder::basic::make_array works", "[bsoncxx::builder::basic::make_array]") {
    auto array = builder::basic::make_array("foo", 1, true);
    auto array_view = array.view();

    REQUIRE(std::distance(array_view.begin(), array_view.end()) == 3);
    REQUIRE(array_view[0].type() == type::k_string);
    REQUIRE(string::to_string(array_view[0].get_string().value) == "foo");
    REQUIRE(array_view[1].type() == type::k_int32);
    REQUIRE(array_view[1].get_int32().value == 1);
    REQUIRE(array_view[2].type() == type::k_bool);
    REQUIRE(array_view[2].get_bool().value == true);
}

TEST_CASE("stream in a document::view works", "[bsoncxx::builder::stream]") {
    using namespace builder::stream;

    auto sub_doc = builder::stream::document{} << "b" << 1 << finalize;
    auto full_doc = builder::stream::document{} << "a" << sub_doc.view() << finalize;

    REQUIRE(full_doc.view()["a"].type() == bsoncxx::types::b_document::type_id);
    REQUIRE(full_doc.view()["a"]["b"].type() == bsoncxx::types::b_int32::type_id);
    REQUIRE(full_doc.view()["a"]["b"].get_int32().value == 1);
}

TEST_CASE("stream in an array::view works", "[bsoncxx::builder::stream]") {
    using namespace builder::stream;

    auto sub_array = builder::stream::array{} << 1 << 2 << 3 << finalize;
    auto full_doc = builder::stream::document{} << "a" << sub_array.view() << finalize;

    REQUIRE(full_doc.view()["a"].type() == bsoncxx::types::b_array::type_id);
    REQUIRE(full_doc.view()["a"][1].type() == bsoncxx::types::b_int32::type_id);
    REQUIRE(full_doc.view()["a"][1].get_int32().value == 2);
}

TEST_CASE("builder::stream::document throws on consecutive keys", "[bsoncxx::builder::core]") {
    builder::stream::document doc;
    REQUIRE_NOTHROW(doc << "foo"
                        << "bar");
    REQUIRE_NOTHROW(doc << "far");
    REQUIRE_THROWS_AS(doc << "boo", bsoncxx::exception);
}

TEST_CASE("list builder appends utf8", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_utf8(&expected, "hello", -1, "world", -1);

    SECTION("works with string literals") {
        builder::list b{"hello", "world"};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("works with std::string") {
        builder::list b{"hello", std::string{"world"}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("works with b_string") {
        builder::list b{"hello", types::b_string{"world"}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("works with const char*") {
        const char* world = "world";
        builder::list b{"hello", world};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("works with char*") {
        char* world = const_cast<char*>("world");
        builder::list b{"hello", world};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends double", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_double(&expected, "foo", -1, 1.1);

    SECTION("works with raw float") {
        builder::list b{"foo", 1.1};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("works with b_double") {
        builder::list b{"foo", types::b_double{1.1}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends binary", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_binary(&expected, "foo", -1, BSON_SUBTYPE_BINARY, (uint8_t*)"data", 4);

    builder::list b{"foo", types::b_binary{binary_sub_type::k_binary, 4, (uint8_t*)"data"}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends undefined", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_undefined(&expected, "foo", -1);

    builder::list b{"foo", types::b_undefined{}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends oid", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_oid_t oid;
    bson_oid_init(&oid, NULL);

    bson_append_oid(&expected, "foo", -1, &oid);

    SECTION("b_oid works") {
        builder::list b{"foo", types::b_oid{bsoncxx::oid{(char*)oid.bytes, 12}}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("raw oid works") {
        builder::list b{"foo", bsoncxx::oid{(char*)oid.bytes, 12}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends bool", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    SECTION("b_bool true works") {
        bson_append_bool(&expected, "foo", -1, 1);

        builder::list b{"foo", types::b_bool{true}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("raw true works") {
        bson_append_bool(&expected, "foo", -1, 1);

        builder::list b{"foo", true};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("b_bool false works") {
        bson_append_bool(&expected, "foo", -1, 0);

        builder::list b{"foo", types::b_bool{false}};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("raw false works") {
        bson_append_bool(&expected, "foo", -1, 0);

        builder::list b{"foo", false};

        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends date time", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_date_time(&expected, "foo", -1, 10000);

    builder::list b{"foo", types::b_date{std::chrono::milliseconds{10000}}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends null", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_null(&expected, "foo", -1);

    builder::list b{"foo", types::b_null{}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends regex", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_regex(&expected, "foo", -1, "^foo|bar$", "i");

    builder::list b{"foo", types::b_regex{"^foo|bar$", "i"}};
    bson_eq_object(&expected, b.view().get_document().value);

    b = {"foo", types::bson_value::value{"^foo|bar$", "i"}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends code", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_code(&expected, "foo", -1, "var a = {};");

    builder::list b{"foo", types::b_code{"var a = {};"}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends symbol", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_symbol(&expected, "foo", -1, "data", -1);

    builder::list b{"foo", types::b_symbol{"data"}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends code with scope", "[bsoncxx::builder::list]") {
    bson_t expected, scope;
    bson_init(&expected);
    builder::stream::document scope_builder;

    bson_init(&scope);

    bson_append_int32(&scope, "b", -1, 10);

    scope_builder << "b" << 10;

    bson_append_code_with_scope(&expected, "foo", -1, "var a = b;", &scope);

    builder::list b{"foo", types::b_codewscope{"var a = b;", scope_builder.view()}};

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends int32", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_int32(&expected, "foo", -1, 100);

    SECTION("raw int32") {
        builder::list b{"foo", 100};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("b_int32") {
        builder::list b{"foo", types::b_int32{100}};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends timestamp", "[bsoncxx::builder::list]") {
    types::b_timestamp foo{100, 1000};
    builder::list b{"foo", foo};

    bson_t expected;
    bson_init(&expected);
    bson_append_timestamp(&expected, "foo", -1, foo.timestamp, foo.increment);

    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends int64", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_int64(&expected, "foo", -1, 100);

    SECTION("raw int64") {
        builder::list b{"foo", std::int64_t(100)};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("b_int64") {
        builder::list b{"foo", types::b_int64{100}};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends decimal128", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_decimal128_t d128;
    bson_decimal128_from_string("-1234E+999", &d128);
    bson_append_decimal128(&expected, "foo", -1, &d128);

    SECTION("b_decimal128 works") {
        builder::list b{"foo", types::b_decimal128{"-1234E+999"}};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("raw bsoncxx::decimal128 works") {
        builder::list b{"foo", bsoncxx::decimal128{"-1234E+999"}};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    SECTION("bsoncxx::types::bson_value::view with decimal128 works") {
        auto d = types::b_decimal128{"-1234E+999"};
        auto v = types::bson_value::view{d};

        REQUIRE(v.get_decimal128() == d);

        builder::list b{"foo", v};
        bson_eq_object(&expected, b.view().get_document().value);
    }

    bson_destroy(&expected);
}

TEST_CASE("list builder appends minkey", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_minkey(&expected, "foo", -1);

    builder::list b{"foo", types::b_minkey{}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends maxkey", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_maxkey(&expected, "foo", -1);

    builder::list b{"foo", types::b_maxkey{}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
}

TEST_CASE("list builder appends array", "[bsoncxx::builder::list]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);

    bson_append_utf8(&child, "0", -1, "bar", -1);
    bson_append_array(&expected, "foo", -1, &child);

    builder::list arr{"bar"};

    builder::list b{"foo", arr};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("list builder appends document", "[bsoncxx::builder::list]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);

    bson_append_utf8(&child, "bar", -1, "baz", -1);
    bson_append_document(&expected, "foo", -1, &child);

    builder::list doc{"bar", "baz"};

    builder::list b{"foo", doc};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("list builder appends inline array", "[bsoncxx::builder::list]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);

    bson_append_utf8(&child, "0", -1, "baz", -1);
    bson_append_array(&expected, "foo", -1, &child);

    builder::list b{"foo", {"baz"}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("list builder appends inline document", "[bsoncxx::builder::list]") {
    bson_t expected, child;
    bson_init(&expected);
    bson_init(&child);

    bson_append_utf8(&child, "bar", -1, "baz", -1);
    bson_append_document(&expected, "foo", -1, &child);

    builder::list b{"foo", {"bar", "baz"}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
    bson_destroy(&child);
}

TEST_CASE("list builder appends inline nested", "[bsoncxx::builder::list]") {
    bson_t expected, foo, bar, third;

    bson_init(&expected);
    bson_init(&foo);
    bson_init(&bar);
    bson_init(&third);

    bson_append_utf8(&third, "hello", -1, "world", -1);
    bson_append_int32(&bar, "0", -1, 1);
    bson_append_int32(&bar, "1", -1, 2);
    bson_append_document(&bar, "2", -1, &third);
    bson_append_array(&foo, "bar", -1, &bar);
    bson_append_document(&expected, "foo", -1, &foo);

    builder::list b{"foo", {"bar", {1, 2, {"hello", "world"}}}};
    bson_eq_object(&expected, b.view().get_document().value);

    bson_destroy(&expected);
    bson_destroy(&foo);
    bson_destroy(&bar);
    bson_destroy(&third);
}

TEST_CASE("list builder appends value", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    bson_append_int32(&expected, "foo", -1, 999);

    types::bson_value::value tmp(999);
    builder::list b{"foo", tmp};

    bson_eq_object(&expected, b.view().get_document().value);
    bson_destroy(&expected);
}

TEST_CASE("list builder with explicit type deduction", "[bsoncxx::builder::list]") {
    using builder::list;
    SECTION("array") {
        bson_t expected, array;
        bson_init(&expected);
        bson_init(&array);

        bson_append_utf8(&array, "0", -1, "bar", -1);
        bson_append_int32(&array, "1", -1, 1);
        bson_append_array(&expected, "foo", -1, &array);

        builder::list b{"foo", builder::array{"bar", 1}};
        bson_eq_object(&expected, b.view().get_document().value);

        bson_destroy(&expected);
        bson_destroy(&array);
    }

    SECTION("document") {
        builder::list b;
        auto kvp_regex =
            Catch::Matches("(.*)must be list of key-value pairs(.*)", Catch::CaseSensitive::No);
        REQUIRE_THROWS_WITH((b = builder::document{"foo", 1, 2}), kvp_regex);

        auto type_regex =
            Catch::Matches("(.*)must be string type(.*)int32(.*)", Catch::CaseSensitive::No);
        REQUIRE_THROWS_WITH((b = builder::document{"foo", 1, 2, 4}), type_regex);
    }
}

TEST_CASE("empty list builder", "[bsoncxx::builder::list]") {
    bson_t expected;
    bson_init(&expected);

    builder::list lst = {};
    bson_eq_object(&expected, lst.view().get_document().value);

    builder::document doc = {};
    bson_eq_object(&expected, doc.view().get_document().value);

    builder::array arr = {};
    bson_eq_object(&expected, arr.view().get_array().value);
}
}  // namespace
