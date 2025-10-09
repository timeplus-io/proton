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

#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>

namespace {
TEST_CASE("time_point is converted to b_date and back", "[bsoncxx::types::b_date]") {
    using bsoncxx::types::b_date;
    using std::chrono::milliseconds;
    using std::chrono::system_clock;
    using std::chrono::time_point_cast;

    system_clock::time_point now1, now2;

    now1 = system_clock::now();
    b_date d{now1};
    now2 = d;

    REQUIRE(time_point_cast<milliseconds>(now1) == time_point_cast<milliseconds>(now2));
}

TEST_CASE("time_point is converted to b_date consistently", "[bsoncxx::types::b_date]") {
    using bsoncxx::types::b_date;
    using std::chrono::system_clock;

    system_clock::time_point now;

    now = system_clock::now();
    b_date d1{now};
    b_date d2{now};

    REQUIRE(d1.value == d2.value);
    REQUIRE(d1 == d2);
}

TEST_CASE("time_point is converted to int64 consistently", "[bsoncxx::types::b_date]") {
    using bsoncxx::types::b_date;
    using std::chrono::system_clock;

    b_date d{system_clock::now()};
    std::int64_t unwrapped = d.to_int64();

    REQUIRE(unwrapped == d);
}
}  // namespace
