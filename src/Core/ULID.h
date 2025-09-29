#pragma once

#include <Core/UUID.h>
#include <IO/WriteHelpers.h>

#if USE_ULID

#include <ulid.h>

namespace DB
{

struct ULIDHelpers
{
    ULIDHelpers() { ulid_generator_init(&generator, 0); }

    std::string generate()
    {
        std::string id;
        id.resize(26);

        ulid_generate(&generator, id.data());

        return id;
    }

private:
    ulid_generator generator;
};

}

#else

namespace DB
{

struct ULIDHelpers
{
    ULIDHelpers() { }

    /// Use uuid v7 to simulate ulid, see `generateUUIDv7.cpp`
    std::string generate()
    {
        UUID uuid = UUIDHelpers::generateV4();

        /// Copied from `FillAllRandomPolicy` in `generateUUIDv7.cpp`
        constexpr auto rand_a_bits_count = 12;
        constexpr auto rand_b_bits_count = 62;
        /// bit masks for UUIDv7 components
        constexpr uint64_t variant_2_mask = (2ull << rand_b_bits_count);
        constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
        constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;

        uint64_t timestamp = UTCMilliseconds::now();
        UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & rand_a_bits_mask) | (timestamp << 16) | 0x7000;
        UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_bits_mask) | variant_2_mask;
        return toString(uuid);
    }
};

}

#endif
