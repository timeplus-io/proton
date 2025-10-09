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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/result/gridfs/upload.hpp>

namespace {
using namespace bsoncxx;
using namespace mongocxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("result::gridfs::upload", "[result::gridfs::upload]") {
    instance::current();

    auto oid = types::b_oid{bsoncxx::oid{}};
    result::gridfs::upload upload_result{types::bson_value::view{oid}};

    SECTION("returns correct response") {
        REQUIRE(upload_result.id() == oid);
    }
}

TEST_CASE("result::gridfs::upload owns id", "[result::gridfs::upload]") {
    // Constructing a result::gridfs::upload requires passing in an id, but we don't care what it
    // is.
    std::string baz = "baz";
    result::gridfs::upload res{types::bson_value::view{types::b_string{baz}}};

    {
        std::string bar = "bar";
        auto doc = make_document(kvp("foo", types::bson_value::view{types::b_string{bar}}));
        auto id = doc.view()["foo"].get_value();
        res = result::gridfs::upload{id};
    }

    std::string bar = "bar";

    // Because result::gridfs::upload owns its id, we should still be able to correctly read new
    // value for the id.
    REQUIRE(res.id().get_string().value == stdx::string_view(bar));
}

TEST_CASE("result::gridfs::upload equals", "[result::gridfs::upload]") {
    instance::current();

    auto oid = types::b_oid{bsoncxx::oid{}};
    result::gridfs::upload upload_result1{types::bson_value::view{oid}};
    result::gridfs::upload upload_result2{types::bson_value::view{oid}};

    REQUIRE(upload_result1 == upload_result2);
}

TEST_CASE("result::gridfs::upload inequals", "[result::gridfs::upload]") {
    instance::current();

    result::gridfs::upload upload_result1{types::bson_value::view{types::b_oid{bsoncxx::oid{}}}};
    result::gridfs::upload upload_result2{types::bson_value::view{types::b_string{"baz"}}};

    REQUIRE(upload_result1 != upload_result2);
}
}  // namespace
