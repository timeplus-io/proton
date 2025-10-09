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
#include <bsoncxx/json.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/result/delete.hpp>

namespace {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("delete", "[delete][result]") {
    mongocxx::instance::current();

    bsoncxx::builder::basic::document build;
    build.append(kvp("_id", bsoncxx::oid{}), kvp("nRemoved", bsoncxx::types::b_int32{1}));

    mongocxx::result::bulk_write b{bsoncxx::document::value(build.view())};

    mongocxx::result::delete_result delete_result{std::move(b)};

    SECTION("returns correct removed count") {
        REQUIRE(delete_result.deleted_count() == 1);
    }
}

TEST_CASE("delete equals", "[delete][result]") {
    mongocxx::instance::current();

    auto doc =
        make_document(kvp("_id", bsoncxx::oid{}), kvp("nRemoved", bsoncxx::types::b_int32{1}));

    mongocxx::result::bulk_write a{doc};
    mongocxx::result::bulk_write b{doc};

    mongocxx::result::delete_result delete_result1{std::move(a)};
    mongocxx::result::delete_result delete_result2{std::move(b)};

    REQUIRE(delete_result1 == delete_result2);
}

TEST_CASE("delete inequals", "[delete][result]") {
    mongocxx::instance::current();

    mongocxx::result::bulk_write a{
        make_document(kvp("_id", bsoncxx::oid{}), kvp("nRemoved", bsoncxx::types::b_int32{1}))};
    mongocxx::result::bulk_write b{
        make_document(kvp("_id", bsoncxx::oid{}), kvp("nRemoved", bsoncxx::types::b_int32{2}))};

    mongocxx::result::delete_result delete_result1{std::move(a)};
    mongocxx::result::delete_result delete_result2{std::move(b)};

    REQUIRE(delete_result1 != delete_result2);
}

}  // namespace
