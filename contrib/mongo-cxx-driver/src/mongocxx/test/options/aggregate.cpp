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
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx::builder::basic;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("aggregate", "[aggregate][option]") {
    mongocxx::instance::current();

    mongocxx::options::aggregate agg;

    const auto collation = make_document(kvp("locale", "en_US"));
    const auto comment = make_document(kvp("$comment", "some_comment"));
    const auto hint = bsoncxx::document::view_or_value(make_document(kvp("_id", 1)));
    const auto let = make_document(kvp("x", "foo"));

    // Avoid error: use of overloaded operator '==' is ambiguous.
    const auto comment_value =
        bsoncxx::types::bson_value::view_or_value(comment["$comment"].get_value());

    CHECK_OPTIONAL_ARGUMENT(agg, allow_disk_use, true);
    CHECK_OPTIONAL_ARGUMENT(agg, batch_size, 500);
    CHECK_OPTIONAL_ARGUMENT(agg, bypass_document_validation, true);
    CHECK_OPTIONAL_ARGUMENT(agg, collation, collation.view());
    CHECK_OPTIONAL_ARGUMENT(agg, comment, comment_value);
    CHECK_OPTIONAL_ARGUMENT(agg, hint, hint);
    CHECK_OPTIONAL_ARGUMENT(agg, let, let.view());
    CHECK_OPTIONAL_ARGUMENT(agg, max_time, std::chrono::milliseconds{1000});
    CHECK_OPTIONAL_ARGUMENT(agg, read_concern, mongocxx::read_concern());
    CHECK_OPTIONAL_ARGUMENT(agg, read_preference, mongocxx::read_preference());
    CHECK_OPTIONAL_ARGUMENT(agg, write_concern, mongocxx::write_concern());
}
}  // namespace
