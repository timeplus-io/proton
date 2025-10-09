// Copyright 2020 MongoDB Inc.
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
#include <cstring>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdlib.h>

#include <bsoncxx/oid.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/test/catch.hh>

using namespace bsoncxx;

namespace {

struct parsed_oid {
    uint32_t timestamp;
    uint64_t rand;
    uint32_t counter;
};

parsed_oid parse_oid(const oid& oid) {
    parsed_oid parsed{};

    // Parse into component sections
    auto bytes = oid.bytes();
    std::memcpy(&parsed.timestamp, bytes, 4);
    std::memcpy(&parsed.rand, bytes + 1, 8);
    std::memcpy(&parsed.counter, bytes + 8, 4);

#if BSON_BYTE_ORDER != BSON_BIG_ENDIAN
#ifndef _WIN32
    parsed.timestamp = __builtin_bswap32(parsed.timestamp);
    parsed.rand = __builtin_bswap64(parsed.rand) & 0x000000FFFFFFFFFF;
    parsed.counter = __builtin_bswap32(parsed.counter) & 0x00FFFFFF;
#else
    parsed.timestamp = _byteswap_ulong(parsed.timestamp);
    parsed.rand = _byteswap_uint64(parsed.rand) & 0x000000FFFFFFFFFF;
    parsed.counter = _byteswap_ulong(parsed.counter) & 0x00FFFFFF;
#endif
#endif

    return parsed;
}

void compare_string(const std::time_t& t, std::string time) {
    char time_str[48];

    REQUIRE(0 != (strftime(time_str, sizeof(time_str), "%b %e, %Y %H:%M:%S UTC", std::gmtime(&t))));

    REQUIRE(time_str == time);
}

TEST_CASE("oid", "[bsoncxx::oid]") {
    SECTION(
        "represents the Timestamp field as an unsigned 32-bit representing the number of seconds "
        "since the Epoch") {
        // 0x00000000: To match "Jan 1st, 1970 00:00:00 UTC"
        oid a{"000000000000000000000000"};
        auto t = a.get_time_t();
        REQUIRE(t == 0x00000000);
        compare_string(t, "Jan  1, 1970 00:00:00 UTC");

        // 0x7FFFFFFF: To match "Jan 19th, 2038 03:14:07 UTC"
        oid b{"7FFFFFFF0000000000000000"};
        t = b.get_time_t();
        REQUIRE(t == 0x7FFFFFFF);
        compare_string(t, "Jan 19, 2038 03:14:07 UTC");

        // 0x80000000: To match "Jan 19th, 2038 03:14:08 UTC"
        oid c{"800000000000000000000000"};
        t = c.get_time_t();
        REQUIRE(t == 0x80000000);
        compare_string(t, "Jan 19, 2038 03:14:08 UTC");

        // 0xFFFFFFFF: To match "Feb 7th, 2106 06:28:15 UTC"
        oid d{"FFFFFFFF0000000000000000"};
        t = d.get_time_t();
        REQUIRE(t == 0xFFFFFFFF);
        compare_string(t, "Feb  7, 2106 06:28:15 UTC");
    }

    SECTION("overflows predictably") {
        /* The C++ driver does not have the ability to set a context for oids, so we
           rely on the C driver's testing of this behaviour */
    }

    SECTION("oid", "uses a different machine/process number after fork is called") {
        /* Ensure that after a new process is created through a fork() or similar
           process creation operation, the "random number unique to a machine and
           process" is no longer the same as the parent process that created
           the new process. */
        bsoncxx::oid oid1{};
        bsoncxx::oid oid2{};
        auto parsed1 = parse_oid(oid1);
        auto parsed2 = parse_oid(oid2);

        // Two oids created in the same process should have the same random number
        REQUIRE(parsed1.rand == parsed2.rand);
        REQUIRE(parsed2.counter == parsed1.counter + 1);

        // Unfortunately, the catch framework does not support forking at this time:
        // https://github.com/catchorg/Catch2/issues/853
        //
        // We rely on the C driver's testing of this behavior.
    }
}

}  // namespace
