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
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/result/insert_one.hpp>

namespace {
using namespace bsoncxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("insert_one", "[insert_one][result]") {
    mongocxx::instance::current();

    bsoncxx::builder::basic::document build;
    auto oid = types::b_oid{bsoncxx::oid{}};
    build.append(kvp("_id", oid), kvp("x", 1));

    mongocxx::result::bulk_write b{document::value(build.view())};

    mongocxx::result::insert_one insert_one{std::move(b), types::bson_value::view{oid}};

    SECTION("returns correct response") {
        REQUIRE(insert_one.inserted_id() == oid);
    }
}

TEST_CASE("insert_one equals", "[insert_one][result]") {
    mongocxx::instance::current();

    auto oid = types::b_oid{bsoncxx::oid{}};
    auto build = make_document(kvp("_id", oid), kvp("x", 1));

    mongocxx::result::bulk_write a{build};
    mongocxx::result::bulk_write b{build};

    mongocxx::result::insert_one insert_one1{std::move(a), types::bson_value::view{oid}};
    mongocxx::result::insert_one insert_one2{std::move(b), types::bson_value::view{oid}};

    REQUIRE(insert_one1 == insert_one2);
}

TEST_CASE("insert_one inequals", "[insert_one][result]") {
    mongocxx::instance::current();

    auto oid = types::b_oid{bsoncxx::oid{}};

    mongocxx::result::bulk_write a{make_document(kvp("_id", oid), kvp("x", 1))};
    mongocxx::result::bulk_write b{make_document(kvp("_id", oid), kvp("x", 2))};

    mongocxx::result::insert_one insert_one1{std::move(a), types::bson_value::view{oid}};
    mongocxx::result::insert_one insert_one2{std::move(b), types::bson_value::view{oid}};

    REQUIRE(insert_one1 != insert_one2);
}

}  // namespace
