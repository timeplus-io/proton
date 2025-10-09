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

#include <fstream>

#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/test/spec/util.hh>

namespace {

using namespace mongocxx;
using namespace spec;

void _run_crud_tests_in_file(std::string test_path) {
    return run_crud_tests_in_file(test_path, uri{});
}

TEST_CASE("CRUD legacy spec automated tests", "[crud_spec]") {
    instance::current();

    run_tests_in_suite("CRUD_LEGACY_TESTS_PATH", _run_crud_tests_in_file);
}
}  // namespace
