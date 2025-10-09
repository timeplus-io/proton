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

#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/test/spec/util.hh>

namespace {

using namespace mongocxx;
using namespace spec;

TEST_CASE("Transactions spec automated tests", "[transactions_spec]") {
    instance::current();

    // Tests that use operations that the C++ driver does not have.
    std::set<std::string> unsupported_transaction_tests = {
        // C Driver does not support count helper.
        "count.json",
    };

    SECTION("Legacy") {
        run_tests_in_suite("TRANSACTIONS_LEGACY_TESTS_PATH",
                           &run_transactions_tests_in_file,
                           unsupported_transaction_tests);
    }

    SECTION("Convenient API") {
        run_tests_in_suite("WITH_TRANSACTION_TESTS_PATH", &run_transactions_tests_in_file);
    }
}

}  // namespace
