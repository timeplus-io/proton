// Copyright 2017 MongoDB Inc.
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
#include <mongocxx/private/conversions.hh>
#include <mongocxx/read_preference.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("libmongoc::conversions::read_mode_t_from_read_mode works", "[libmongoc::conversions]") {
    REQUIRE(libmongoc::conversions::read_mode_t_from_read_mode(
                read_preference::read_mode::k_primary) == MONGOC_READ_PRIMARY);
    REQUIRE(libmongoc::conversions::read_mode_t_from_read_mode(
                read_preference::read_mode::k_primary_preferred) == MONGOC_READ_PRIMARY_PREFERRED);
    REQUIRE(libmongoc::conversions::read_mode_t_from_read_mode(
                read_preference::read_mode::k_secondary) == MONGOC_READ_SECONDARY);
    REQUIRE(libmongoc::conversions::read_mode_t_from_read_mode(
                read_preference::read_mode::k_secondary_preferred) ==
            MONGOC_READ_SECONDARY_PREFERRED);
    REQUIRE(libmongoc::conversions::read_mode_t_from_read_mode(
                read_preference::read_mode::k_nearest) == MONGOC_READ_NEAREST);
}

TEST_CASE("libmongoc::conversions::read_mode_from_read_mode_t works", "[libmongoc::conversions]") {
    REQUIRE(libmongoc::conversions::read_mode_from_read_mode_t(MONGOC_READ_PRIMARY) ==
            read_preference::read_mode::k_primary);
    REQUIRE(libmongoc::conversions::read_mode_from_read_mode_t(MONGOC_READ_PRIMARY_PREFERRED) ==
            read_preference::read_mode::k_primary_preferred);
    REQUIRE(libmongoc::conversions::read_mode_from_read_mode_t(MONGOC_READ_SECONDARY) ==
            read_preference::read_mode::k_secondary);
    REQUIRE(libmongoc::conversions::read_mode_from_read_mode_t(MONGOC_READ_SECONDARY_PREFERRED) ==
            read_preference::read_mode::k_secondary_preferred);
    REQUIRE(libmongoc::conversions::read_mode_from_read_mode_t(MONGOC_READ_NEAREST) ==
            read_preference::read_mode::k_nearest);
}
}  // namespace
