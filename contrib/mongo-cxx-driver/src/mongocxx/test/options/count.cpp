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

#include <chrono>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/count.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx::builder::basic;
using namespace mongocxx;

TEST_CASE("count", "[count][option]") {
    instance::current();

    options::count cnt;

    auto collation = make_document(kvp("locale", "en_US"));
    auto hint = bsoncxx::document::view_or_value{make_document(kvp("_id", 1))};

    CHECK_OPTIONAL_ARGUMENT(cnt, collation, collation.view());
    CHECK_OPTIONAL_ARGUMENT(cnt, hint, hint);
    CHECK_OPTIONAL_ARGUMENT(cnt, limit, 3);
    CHECK_OPTIONAL_ARGUMENT(cnt, max_time, std::chrono::milliseconds{1000});
    CHECK_OPTIONAL_ARGUMENT(cnt, read_preference, read_preference{});
    CHECK_OPTIONAL_ARGUMENT(cnt, skip, 3);
}

}  // namespace
