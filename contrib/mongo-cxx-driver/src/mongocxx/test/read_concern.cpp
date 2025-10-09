// Copyright 2014 MongoDB Inc.
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

#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/read_concern.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("valid read concern settings", "[read_concern]") {
    instance::current();

    read_concern rc{};

    read_concern::level level_setting = read_concern::level::k_server_default;
    stdx::string_view string_setting{""};

    SECTION("default-constructed read_concern") {
        level_setting = read_concern::level::k_server_default;
        string_setting = stdx::string_view{""};
    }

    SECTION("can be assigned with acknowledge_level()") {
        SECTION("local") {
            level_setting = read_concern::level::k_local;
            string_setting = stdx::string_view{"local"};
        }

        SECTION("majority") {
            level_setting = read_concern::level::k_majority;
            string_setting = stdx::string_view{"majority"};
        }

        SECTION("linearizable") {
            level_setting = read_concern::level::k_linearizable;
            string_setting = stdx::string_view{"linearizable"};
        }

        SECTION("server default") {
            level_setting = read_concern::level::k_server_default;
            string_setting = stdx::string_view{""};
        }

        SECTION("available") {
            level_setting = read_concern::level::k_available;
            string_setting = stdx::string_view{"available"};
        }

        SECTION("server default") {
            level_setting = read_concern::level::k_snapshot;
            string_setting = stdx::string_view{"snapshot"};
        }

        REQUIRE_NOTHROW(rc.acknowledge_level(level_setting));
    }

    SECTION("can be assigned with acknowledge_string()") {
        SECTION("local") {
            level_setting = read_concern::level::k_local;
            string_setting = stdx::string_view{"local"};
        }

        SECTION("majority") {
            level_setting = read_concern::level::k_majority;
            string_setting = stdx::string_view{"majority"};
        }

        SECTION("linearizable") {
            level_setting = read_concern::level::k_linearizable;
            string_setting = stdx::string_view{"linearizable"};
        }

        SECTION("server default") {
            level_setting = read_concern::level::k_server_default;
            string_setting = stdx::string_view{""};
        }

        SECTION("available") {
            level_setting = read_concern::level::k_available;
            string_setting = stdx::string_view{"available"};
        }

        SECTION("server default") {
            level_setting = read_concern::level::k_snapshot;
            string_setting = stdx::string_view{"snapshot"};
        }

        REQUIRE_NOTHROW(rc.acknowledge_string(string_setting));
    }

    REQUIRE(rc.acknowledge_level() == level_setting);
    REQUIRE(rc.acknowledge_string() == string_setting);
}

TEST_CASE("setting the string to an unknown value changes the level to unknown", "[read_concern]") {
    instance::current();

    read_concern rc{};

    rc.acknowledge_string("futureCompatible");
    REQUIRE(rc.acknowledge_level() == read_concern::level::k_unknown);
}

TEST_CASE("read_concern throws when trying to set level to k_unknown", "[read_concern]") {
    instance::current();

    read_concern rc{};

    REQUIRE_THROWS(rc.acknowledge_level(read_concern::level::k_unknown));
}

TEST_CASE("read_concern equality operator works", "[read_concern]") {
    instance::current();

    read_concern rc_a{};
    read_concern rc_b{};

    SECTION("default-constructed read_concern objects are equal") {
        REQUIRE(rc_a == rc_b);
    }

    SECTION("acknowledge_level is compared") {
        SECTION("with known level") {
            rc_a.acknowledge_level(read_concern::level::k_local);
            REQUIRE_FALSE(rc_a == rc_b);
            rc_b.acknowledge_level(read_concern::level::k_local);
            REQUIRE(rc_a == rc_b);
        }

        SECTION("with unknown level") {
            rc_a.acknowledge_string("foo");
            REQUIRE_FALSE(rc_a == rc_b);
            rc_b.acknowledge_string("bar");
            REQUIRE(rc_a == rc_b);
        }
    }
}

TEST_CASE("read_concern inequality operator works", "[read_concern]") {
    instance::current();

    read_concern rc_a{};
    read_concern rc_b{};

    REQUIRE_FALSE(rc_a != rc_b);
    rc_a.acknowledge_level(read_concern::level::k_local);
    REQUIRE(rc_a != rc_b);
}
}  // namespace
