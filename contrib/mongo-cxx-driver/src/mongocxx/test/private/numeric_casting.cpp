// Copyright 2022-present MongoDB Inc.
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
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/numeric_casting.hh>

namespace {

TEST_CASE("can do basic numeric_casting", "[numeric_casting]") {
    int64_t i64 = 0;
    int32_t i32 = 0;
    std::size_t sz = 0;

    SECTION("size_t_to_int64_safe") {
        REQUIRE(mongocxx::size_t_to_int64_safe(0, i64));
        REQUIRE(i64 == 0);
        REQUIRE(mongocxx::size_t_to_int64_safe(123, i64));
        REQUIRE(i64 == 123);
    }

    SECTION("int64_to_int32_safe") {
        REQUIRE(mongocxx::int64_to_int32_safe(0, i32));
        REQUIRE(i32 == 0);
        REQUIRE(mongocxx::int64_to_int32_safe(123, i32));
        REQUIRE(i32 == 123);
        REQUIRE_FALSE(mongocxx::int64_to_int32_safe(std::numeric_limits<int64_t>::max(), i32));
        REQUIRE_FALSE(mongocxx::int64_to_int32_safe(std::numeric_limits<int64_t>::min(), i32));
        REQUIRE_FALSE(mongocxx::int64_to_int32_safe(
            static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1, i32));
        REQUIRE_FALSE(mongocxx::int64_to_int32_safe(
            static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1, i32));
    }

    SECTION("int32_to_size_t_safe") {
        REQUIRE(mongocxx::int32_to_size_t_safe(0, sz));
        REQUIRE(sz == 0);
        REQUIRE(mongocxx::int32_to_size_t_safe(123, sz));
        REQUIRE(sz == 123);
        REQUIRE_FALSE(mongocxx::int32_to_size_t_safe(-1, sz));
    }

    SECTION("int64_to_size_t_safe") {
        REQUIRE(mongocxx::int64_to_size_t_safe(0, sz));
        REQUIRE(sz == 0);
        REQUIRE(mongocxx::int64_to_size_t_safe(123, sz));
        REQUIRE(sz == 123);
        REQUIRE_FALSE(mongocxx::int64_to_size_t_safe(-1, sz));
    }
}
}  // namespace
