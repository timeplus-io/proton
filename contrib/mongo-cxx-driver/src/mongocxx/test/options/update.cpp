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
#include <mongocxx/options/update.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx::builder::basic;
using namespace mongocxx;

TEST_CASE("update opts", "[update][option]") {
    instance::current();

    options::update updt;

    auto collation = make_document(kvp("locale", "en_US"));

    CHECK_OPTIONAL_ARGUMENT(updt, bypass_document_validation, true);
    CHECK_OPTIONAL_ARGUMENT(updt, collation, collation.view());
    CHECK_OPTIONAL_ARGUMENT(updt, upsert, true);
    CHECK_OPTIONAL_ARGUMENT(updt, write_concern, write_concern{});
}
}  // namespace
