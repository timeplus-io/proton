// Copyright 2018-present MongoDB Inc.
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
#include <mongocxx/options/replace.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx::builder::basic;
using namespace mongocxx;

TEST_CASE("replace opts", "[replace][option]") {
    instance::current();

    options::replace repl;

    auto collation = make_document(kvp("locale", "en_US"));

    CHECK_OPTIONAL_ARGUMENT(repl, bypass_document_validation, true);
    CHECK_OPTIONAL_ARGUMENT(repl, collation, collation.view());
    CHECK_OPTIONAL_ARGUMENT(repl, upsert, true);
    CHECK_OPTIONAL_ARGUMENT(repl, write_concern, write_concern{});
}
}  // namespace
