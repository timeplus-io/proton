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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/result/update.hpp>

namespace {
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("update", "[update][result]") {
    mongocxx::instance::current();

    bsoncxx::builder::basic::document build;
    build.append(kvp("_id", bsoncxx::oid{}),
                 kvp("nMatched", bsoncxx::types::b_int32{2}),
                 kvp("nModified", bsoncxx::types::b_int32{1}));

    mongocxx::result::bulk_write b{bsoncxx::document::value(build.view())};

    mongocxx::result::update update{std::move(b)};

    SECTION("returns correct matched and modified count") {
        REQUIRE(update.matched_count() == 2);
        REQUIRE(update.modified_count() == 1);
    }
}

TEST_CASE("update result equals", "[update][result]") {
    mongocxx::instance::current();

    auto doc = make_document(kvp("_id", bsoncxx::oid{}),
                             kvp("nMatched", bsoncxx::types::b_int32{2}),
                             kvp("nModified", bsoncxx::types::b_int32{1}));

    mongocxx::result::bulk_write a{doc};
    mongocxx::result::bulk_write b{doc};

    mongocxx::result::update update1{std::move(a)};
    mongocxx::result::update update2{std::move(b)};

    REQUIRE(update1 == update2);
}

TEST_CASE("update result inequals", "[update][result]") {
    mongocxx::instance::current();

    mongocxx::result::bulk_write a{make_document(kvp("_id", bsoncxx::oid{}),
                                                 kvp("nMatched", bsoncxx::types::b_int32{2}),
                                                 kvp("nModified", bsoncxx::types::b_int32{1}))};
    mongocxx::result::bulk_write b{make_document(kvp("_id", bsoncxx::oid{}),
                                                 kvp("nMatched", bsoncxx::types::b_int32{3}),
                                                 kvp("nModified", bsoncxx::types::b_int32{1}))};

    mongocxx::result::update update1{std::move(a)};
    mongocxx::result::update update2{std::move(b)};

    REQUIRE(update1 != update2);
}

}  // namespace
