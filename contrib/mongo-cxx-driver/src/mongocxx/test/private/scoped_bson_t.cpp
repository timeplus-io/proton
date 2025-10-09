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
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/private/libbson.hh>

namespace {
using namespace bsoncxx;
using namespace mongocxx;
using namespace mongocxx::libbson;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("scoped_bson_t", "[private]") {
    SECTION("Can be constructed") {
        document::value val = make_document(kvp("a", 1));
        document::view view = val.view();
        document::view empty_view;

        SECTION("from a view") {
            scoped_bson_t bson{view};
            REQUIRE(bson.view() == view);
        }

        SECTION("from a value") {
            document::value copy{val};
            scoped_bson_t bson{std::move(copy)};
            REQUIRE(bson.view() == view);
        }

        SECTION("from a temporary value") {
            scoped_bson_t bson{make_document(kvp("a", 1))};
            REQUIRE(bson.view() == view);
        }

        SECTION("from a view_or_value") {
            document::view_or_value variant{val};
            scoped_bson_t bson{variant};
            REQUIRE(bson.view() == view);
        }

        SECTION("from an empty optional view_or_value") {
            bsoncxx::stdx::optional<document::view_or_value> empty;
            scoped_bson_t bson{empty};
            REQUIRE(bson.view() == empty_view);
        }

        SECTION("from an engaged optional view_or_value") {
            document::view_or_value variant{val};
            bsoncxx::stdx::optional<document::view_or_value> engaged{variant};
            scoped_bson_t bson{engaged};
            REQUIRE(bson.view() == view);
        }
    }

    SECTION("Can be initialized") {
        auto val = make_document(kvp("a", 1));
        auto view = val.view();
        document::view empty_view;

        SECTION("from a view") {
            scoped_bson_t bson;
            bson.init_from_static(view);
            REQUIRE(bson.view() == view);
        }

        SECTION("from a value") {
            scoped_bson_t bson;
            document::value copy{val};
            bson.init_from_static(std::move(copy));
            REQUIRE(bson.view() == view);
        }

        SECTION("from a temporary value") {
            scoped_bson_t bson;
            bson.init_from_static(make_document(kvp("a", 1)));
            REQUIRE(bson.view() == view);
        }

        SECTION("from a view_or_value") {
            scoped_bson_t bson;
            document::view_or_value variant{val};
            bson.init_from_static(variant);
            REQUIRE(bson.view() == view);
        }

        SECTION("from an empty optional view_or_value") {
            scoped_bson_t bson;
            bsoncxx::stdx::optional<document::view_or_value> empty;
            bson.init_from_static(empty);
            REQUIRE(bson.view() == empty_view);
        }

        SECTION("from an engaged optional view_or_value") {
            scoped_bson_t bson;
            document::view_or_value variant{val};
            bsoncxx::stdx::optional<document::view_or_value> engaged{variant};
            bson.init_from_static(engaged);
            REQUIRE(bson.view() == view);
        }
    }
}
}  // namespace
