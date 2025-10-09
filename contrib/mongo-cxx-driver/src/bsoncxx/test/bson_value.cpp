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

#include <algorithm>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types/bson_value/make_value.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

namespace {
using namespace bsoncxx;

using bsoncxx::to_json;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using namespace bsoncxx::types;

namespace {

void value_construction_test(bson_value::view test_view) {
    bson_value::value test_value{test_view};

    REQUIRE(test_value == test_view);
}

template <typename T, typename U>
void coverting_construction_test(T actual, U expected) {
    bson_value::value copy_initialization = actual;
    REQUIRE(copy_initialization == expected);

    bson_value::value direct_initialization(actual);
    REQUIRE(direct_initialization == expected);

    bson_value::value _static_cast = static_cast<bson_value::value>(actual);
    REQUIRE(_static_cast == expected);
}

}  // namespace

TEST_CASE("types::bson_value::value", "[bsoncxx::types::bson_value::value]") {
    SECTION("can be constructed from") {
        SECTION("bool") {
            auto test_value = bson_value::make_value(b_bool{true});
            value_construction_test(test_value.view());

            coverting_construction_test(true, test_value);
            coverting_construction_test(b_bool{true}, test_value);
        }

        SECTION("string") {
            std::string value = "super duper";

            auto test_doc = bson_value::make_value(value);
            value_construction_test(test_doc.view());

            coverting_construction_test(value, test_doc);
            coverting_construction_test(value.c_str(), test_doc);
            coverting_construction_test(b_string{value}, test_doc);
            coverting_construction_test(stdx::string_view{value}, test_doc);

            const char raw_data[]{'s', 'u', 'p', 'e', 'r', ' ', 'd', 'u', 'p', 'e', 'r'};
            coverting_construction_test(stdx::string_view{raw_data, 11}, test_doc);

            auto test_empty = bson_value::make_value("");
            value_construction_test(test_empty.view());
            coverting_construction_test("", test_empty);

            auto nulls = "a\0\0\0";
            auto test_nulls = bson_value::make_value(nulls);
            value_construction_test(test_nulls.view());
            coverting_construction_test(nulls, test_nulls);
        }

        SECTION("double") {
            auto lower_bound = std::numeric_limits<double>::min();
            auto test_doc = bson_value::make_value(b_double{lower_bound});
            value_construction_test(test_doc.view());

            coverting_construction_test(lower_bound, test_doc);
            coverting_construction_test(b_double{lower_bound}, test_doc);
        }

        SECTION("int32") {
            auto lower_bound = std::numeric_limits<int32_t>::min();
            auto test_doc = bson_value::make_value(b_int32{lower_bound});
            value_construction_test(test_doc.view());

            coverting_construction_test(lower_bound, test_doc);
            coverting_construction_test(b_int32{lower_bound}, test_doc);
        }

        SECTION("int64") {
            auto lower_bound = std::numeric_limits<int64_t>::min();
            auto test_doc = bson_value::make_value(b_int64{lower_bound});
            value_construction_test(test_doc.view());

            coverting_construction_test(lower_bound, test_doc);
            coverting_construction_test(b_int64{lower_bound}, test_doc);
        }

        SECTION("undefined") {
            auto test_doc = bson_value::make_value(b_undefined{});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_undefined{}, test_doc);
            REQUIRE(bson_value::value(type::k_undefined) == test_doc);
        }

        SECTION("oid") {
            oid id{"0123456789abcdefABCDEFFF"};
            auto test_doc = bson_value::make_value(b_oid{id});
            value_construction_test(test_doc.view());

            coverting_construction_test(id, test_doc);
            coverting_construction_test(b_oid{id}, test_doc);
        }

        SECTION("decimal128") {
            uint64_t high = std::numeric_limits<uint64_t>::max();
            uint64_t low = std::numeric_limits<uint64_t>::min();

            auto test_doc = bson_value::make_value(b_decimal128{decimal128{high, low}});
            value_construction_test(test_doc.view());

            coverting_construction_test(decimal128{high, low}, test_doc);
            coverting_construction_test(b_decimal128{decimal128{high, low}}, test_doc);
            REQUIRE(bson_value::value(type::k_decimal128, high, low) == test_doc);
        }

        SECTION("date") {
            auto test_doc = bson_value::make_value(b_date(std::chrono::milliseconds(123456789)));
            value_construction_test(test_doc.view());

            coverting_construction_test(std::chrono::milliseconds(123456789), test_doc);
            coverting_construction_test(b_date(std::chrono::milliseconds(123456789)), test_doc);
        }

        SECTION("null") {
            auto test_doc = bson_value::make_value(b_null{});
            value_construction_test(test_doc.view());

            coverting_construction_test(nullptr, test_doc);
            coverting_construction_test(b_null{}, test_doc);
        }

        SECTION("regex") {
            auto regex = "amy";
            /* options are sorted and any duplicate or invalid options are removed */
            auto options = "imsx";

            auto test_doc = bson_value::make_value(b_regex{regex, options});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_regex{regex, options}, test_doc);
            REQUIRE(bson_value::value(regex, options) == test_doc);

            auto empty_regex = bson_value::make_value(b_regex{""});
            value_construction_test(empty_regex.view());

            coverting_construction_test(b_regex{""}, empty_regex);
            REQUIRE(bson_value::value(type::k_regex, "") == empty_regex);
            REQUIRE(bson_value::value("", "") == empty_regex);
        }

        SECTION("dbpointer") {
            oid id{"0123456789abcdefABCDEFFF"};
            auto coll_name = "collection";

            auto test_doc = bson_value::make_value(b_dbpointer{coll_name, id});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_dbpointer{coll_name, id}, test_doc);
            REQUIRE(bson_value::value(coll_name, id) == test_doc);

            auto empty_oid = oid{};
            auto empty_collection = bson_value::make_value(b_dbpointer{"", empty_oid});
            value_construction_test(empty_collection.view());

            coverting_construction_test(b_dbpointer{"", empty_oid}, empty_collection);
            REQUIRE(bson_value::value("", empty_oid) == empty_collection);
        }

        SECTION("code") {
            auto code = "look at me I'm some JS code";
            auto test_doc = bson_value::make_value(b_code{code});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_code{code}, test_doc);
            REQUIRE(bson_value::value(type::k_code, code) == test_doc);

            auto empty_code = bson_value::make_value(b_code{""});
            value_construction_test(empty_code.view());
            REQUIRE(bson_value::value(type::k_code, "") == empty_code);
        }

        SECTION("codewscope") {
            auto doc = make_document(kvp("a", "b"));
            auto code = "it's me, Code with Scope";
            auto test_doc = bson_value::make_value(b_codewscope{code, doc.view()});

            value_construction_test(test_doc.view());
            coverting_construction_test(b_codewscope{code, doc.view()}, test_doc);
            REQUIRE(bson_value::value(code, doc.view()) == test_doc);

            auto empty_doc = make_document(kvp("a", ""));
            auto empty_code = bson_value::make_value(b_codewscope{"", empty_doc.view()});
            value_construction_test(empty_code.view());

            coverting_construction_test(b_codewscope{"", empty_doc.view()}, empty_code);
            REQUIRE(bson_value::value("", empty_doc.view()) == empty_code);
        }

        SECTION("minkey") {
            auto test_doc = bson_value::make_value(b_minkey{});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_minkey{}, test_doc);
            REQUIRE(bson_value::value(type::k_minkey) == test_doc);
        }

        SECTION("maxkey") {
            auto test_doc = bson_value::make_value(b_maxkey{});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_maxkey{}, test_doc);
            REQUIRE(bson_value::value(type::k_maxkey) == test_doc);
        }

        SECTION("binary") {
            std::vector<uint8_t> bin{'d', 'e', 'a', 'd', 'b', 'e', 'e', 'f'};
            auto test_doc = bson_value::make_value(
                b_binary{binary_sub_type::k_binary, (uint32_t)bin.size(), bin.data()});
            value_construction_test(test_doc.view());

            coverting_construction_test(bin, test_doc);
            REQUIRE(bson_value::value(b_binary{
                        binary_sub_type::k_binary, (uint32_t)bin.size(), bin.data()}) == test_doc);
            REQUIRE(bson_value::value(bin.data(), bin.size(), binary_sub_type::k_binary) ==
                    test_doc);
            REQUIRE(bson_value::value(bin.data(), bin.size()) == test_doc);

            auto empty = bson_value::make_value(b_binary{});
            coverting_construction_test(std::vector<unsigned char>{}, empty);
        }

        SECTION("symbol") {
            auto symbol = "some symbol";
            auto test_doc = bson_value::make_value(b_symbol{symbol});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_symbol{symbol}, test_doc);
            REQUIRE(bson_value::value(type::k_symbol, symbol) == test_doc);

            auto empty_symbol = bson_value::make_value(b_symbol{""});
            value_construction_test(empty_symbol.view());

            REQUIRE(bson_value::value(type::k_symbol, "") == empty_symbol);
        }

        SECTION("timestamp") {
            uint32_t inc = std::numeric_limits<uint32_t>::max();
            uint32_t time = std::numeric_limits<uint32_t>::max() - 1;

            auto test_doc = bson_value::make_value(b_timestamp{inc, time});
            value_construction_test(test_doc.view());

            coverting_construction_test(b_timestamp{inc, time}, test_doc);
            REQUIRE(bson_value::value(type::k_timestamp, inc, time) == test_doc);
        }

        SECTION("document") {
            auto doc = make_document(kvp("a", 1));
            auto test_doc = bson_value::make_value(doc.view());
            value_construction_test(test_doc.view());

            coverting_construction_test(b_document{doc.view()}, test_doc);
            REQUIRE(bson_value::value(doc.view()) == test_doc);

            auto empty_doc = bson_value::make_value(document::view{});
            value_construction_test(empty_doc.view());

            coverting_construction_test(b_document{}, empty_doc);
            coverting_construction_test(document::view{}, empty_doc);
        }

        SECTION("array") {
            auto arr = make_array(make_document(kvp("hi", 0)));
            auto test_doc = bson_value::make_value(arr.view());
            value_construction_test(test_doc.view());

            coverting_construction_test(b_array{arr.view()}, test_doc);
            coverting_construction_test(arr.view(), test_doc);

            auto empty_doc = bson_value::make_value(array::view{});
            value_construction_test(empty_doc.view());

            coverting_construction_test(b_array{}, empty_doc);
            coverting_construction_test(array::view{}, empty_doc);
        }
    }

    SECTION("can be constructed by a document::element") {
        auto doc_value = make_document(kvp("hello", "world"));
        auto doc2_value = make_document(kvp("a", 1));

        auto doc = doc_value.view();
        auto doc2 = doc2_value.view();

        auto elem = doc["hello"];
        auto elem2 = doc2["a"];

        bson_value::value value = elem.get_owning_value();

        SECTION("can create new views") {
            bson_value::view view(value);
            bson_value::view new_view(value);
            REQUIRE(view == new_view);
        }

        SECTION("can be copy constructed") {
            bson_value::value copied{value};
            REQUIRE(value == copied);
        }

        SECTION("can be copy assigned") {
            bson_value::value copied = elem2.get_owning_value();
            REQUIRE(value != copied);
            {
                bson_value::value temp = elem.get_owning_value();
                copied = temp;
            }
            REQUIRE(value == copied);
        }

        SECTION("can be move constructed") {
            bson_value::value temp = elem.get_owning_value();
            bson_value::value moved{std::move(temp)};
            REQUIRE(moved == value);
        }

        SECTION("can be move assigned") {
            bson_value::value moved = elem2.get_owning_value();
            {
                bson_value::value temp = elem.get_owning_value();
                moved = std::move(temp);
            }
            REQUIRE(moved == value);
        }

        SECTION("Owns its own memory buffer") {
            bson_value::value moved = elem2.get_owning_value();
            bson_value::value original = elem.get_owning_value();

            {
                // Matches "doc" but has distinct memory.
                auto tempdoc = make_document(kvp("hello", "world"));
                auto tempview = tempdoc.view();
                auto temp = tempview["hello"].get_owning_value();

                moved = std::move(temp);
            }

            REQUIRE(moved == original);
        }

        SECTION("Can be compared to another bson_value::value") {
            SECTION("Compares equal with equal views, regardless of ownership") {
                bson_value::value temp = elem.get_owning_value();
                bson_value::value a{std::move(temp)};
                bson_value::value b = elem.get_owning_value();

                REQUIRE(b == a);
            }

            SECTION("Compares inequal with different views") {
                bson_value::value a = elem.get_owning_value();
                bson_value::value b = elem2.get_owning_value();

                REQUIRE(a != b);
            }
        }

        SECTION("Can be compared to a bson_value::view") {
            auto bad_doc = make_document(kvp("blah", 1));
            auto bad_doc_view = bad_doc.view();
            auto bad_view = bad_doc_view["blah"].get_value();
            auto view = elem.get_value();

            bson_value::value a = elem.get_owning_value();

            REQUIRE(a == view);
            REQUIRE(view == a);
            REQUIRE(a != bad_view);
            REQUIRE(bad_view != a);
        }
    }
}

}  // namespace
