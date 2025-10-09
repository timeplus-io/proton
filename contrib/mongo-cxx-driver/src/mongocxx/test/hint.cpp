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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/hint.hpp>
#include <mongocxx/instance.hpp>

namespace {
using namespace mongocxx;
using namespace bsoncxx;

using bsoncxx::builder::concatenate;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("Hint", "[hint]") {
    instance::current();

    SECTION("Can be constructed with index name") {
        std::string index_name = "a_1";
        hint index_hint{index_name};

        SECTION("Returns correct value from to_value") {
            types::bson_value::view val = index_hint.to_value();
            REQUIRE(val.type() == type::k_string);
            REQUIRE(bsoncxx::string::to_string(val.get_string().value) == index_name);
        }

        SECTION("Compares equal to matching index name") {
            REQUIRE(index_hint == index_name);
            REQUIRE(index_name == index_hint);
        }

        SECTION("Does not equal non-matching index name") {
            REQUIRE(index_hint != "sam");
            REQUIRE("sam" != index_hint);
        }

        SECTION("Does not equal index document") {
            auto index_doc = make_document(kvp("a", 1));
            REQUIRE(index_hint != index_doc);
        }
    }

    SECTION("Can be constructed with index document value") {
        auto index_doc = make_document(kvp("a", 1));
        document::value index_copy{index_doc};

        hint index_hint{std::move(index_doc)};

        SECTION("Returns correct value from to_value") {
            types::bson_value::view val = index_hint.to_value();
            REQUIRE(val.type() == type::k_document);
            REQUIRE(val.get_document().value == index_copy);
        }

        SECTION("Compares equal to matching index doc view or value") {
            REQUIRE(index_hint == index_copy);
            REQUIRE(index_hint == index_copy.view());
            REQUIRE(index_copy == index_hint);
            REQUIRE(index_copy.view() == index_hint);
        }

        SECTION("Does not equal non-matching index doc") {
            auto bad_doc = make_document(kvp("totoro", 1));
            REQUIRE(index_hint != bad_doc);
            REQUIRE(bad_doc != index_hint);
        }

        SECTION("Does not equal index string") {
            REQUIRE(index_hint != "totoro_1");
            REQUIRE("a" != index_hint);
        }
    }

    SECTION("Can be constructed with index document view") {
        auto index_doc = make_document(kvp("a", 1));
        hint index_hint{index_doc.view()};

        SECTION("Compares equal to matching index doc view or value") {
            REQUIRE(index_hint == index_doc);
            REQUIRE(index_hint == index_doc.view());
            REQUIRE(index_doc == index_hint);
            REQUIRE(index_doc.view() == index_hint);
        }
    }
}
}  // namespace
