// Copyright 2015 MongoDB Inc.
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
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/instance.hpp>

namespace {
using namespace mongocxx;

instance* inst;

TEST_CASE("instance::current creates instance when one has not already been created") {
    REQUIRE_NOTHROW(inst = &instance::current());
}

TEST_CASE("multiple instances cannot be created") {
    REQUIRE_THROWS_AS(instance{}, logic_error);
}

TEST_CASE("instance::current works when instance is alive") {
    REQUIRE_NOTHROW(instance::current());
}

TEST_CASE("an instance cannot be created after one has been destroyed") {
    inst->~instance();
    REQUIRE_THROWS_AS(instance{}, logic_error);
}

TEST_CASE("instance::current throws if an instance has already been destroyed") {
    REQUIRE_THROWS_AS(instance::current(), logic_error);
}
}  // namespace
