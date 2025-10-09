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

#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/bulk_write.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("bulk_write opts", "[bulk_write][options]") {
    options::bulk_write bulk_write_opts;

    REQUIRE(bulk_write_opts.ordered());
    CHECK_OPTIONAL_ARGUMENT(bulk_write_opts, write_concern, write_concern{});
    CHECK_OPTIONAL_ARGUMENT(bulk_write_opts, bypass_document_validation, true);
}
}  // namespace
