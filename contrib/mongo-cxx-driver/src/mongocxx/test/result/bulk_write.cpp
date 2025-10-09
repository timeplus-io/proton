// Copyright 2017 MongoDB Inc.
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
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/result/bulk_write.hpp>

namespace {
using namespace bsoncxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

TEST_CASE("bulk_write result", "[bulk_write][result]") {
    mongocxx::instance::current();

    auto oid1 = types::b_oid{bsoncxx::oid{}};
    auto oid2 = types::b_oid{bsoncxx::oid{}};

    auto arr = make_array(make_document(kvp("_id", oid1), kvp("index", 0)),
                          make_document(kvp("_id", oid2), kvp("index", 1)));

    auto build = make_document(kvp("nInserted", 1),
                               kvp("nMatched", 0),
                               kvp("nModified", 3),
                               kvp("nRemoved", 0),
                               kvp("nUpserted", 2),
                               kvp("upserted", arr.view()));

    mongocxx::result::bulk_write bulk_write_res{build};

    REQUIRE(bulk_write_res.inserted_count() == 1);
    REQUIRE(bulk_write_res.matched_count() == 0);
    REQUIRE(bulk_write_res.modified_count() == 3);
    REQUIRE(bulk_write_res.deleted_count() == 0);
    REQUIRE(bulk_write_res.upserted_count() == 2);

    mongocxx::result::bulk_write::id_map upserted = bulk_write_res.upserted_ids();
    REQUIRE(upserted[0].get_oid() == oid1);
    REQUIRE(upserted[1].get_oid() == oid2);
}

TEST_CASE("bulk_write result equals", "[bulk_write][result]") {
    auto build = make_document(kvp("nInserted", 1),
                               kvp("nMatched", 0),
                               kvp("nModified", 3),
                               kvp("nRemoved", 0),
                               kvp("nUpserted", 2));
    mongocxx::result::bulk_write bw1{build};
    mongocxx::result::bulk_write bw2{build};

    REQUIRE(bw1 == bw2);
}

TEST_CASE("bulk_write result inequals", "[bulk_write][result]") {
    auto build1 = make_document(kvp("nInserted", 1),
                                kvp("nMatched", 0),
                                kvp("nModified", 3),
                                kvp("nRemoved", 0),
                                kvp("nUpserted", 2));
    auto build2 = make_document(kvp("nInserted", 0),
                                kvp("nMatched", 2),
                                kvp("nModified", 0),
                                kvp("nRemoved", 2),
                                kvp("nUpserted", 0));
    mongocxx::result::bulk_write bw1{build1};
    mongocxx::result::bulk_write bw2{build2};

    REQUIRE(bw1 != bw2);
}
}  // namespace
