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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/model/delete_one.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx::builder::basic;

TEST_CASE("delete_one model tests", "[delete_one][model]") {
    mongocxx::instance::current();

    auto filter = make_document(kvp("a", 1));
    auto collation = make_document(kvp("locale", "en_US"));

    mongocxx::model::delete_one del_one(filter.view());

    SECTION("stores required arguments") {
        REQUIRE(del_one.filter().view() == filter.view());
    }

    CHECK_OPTIONAL_ARGUMENT(del_one, collation, collation.view());
}
}  // namespace
