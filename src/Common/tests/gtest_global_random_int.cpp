#include <Common/Random.h>

#include <gtest/gtest.h>

TEST(RandomInt, SignedInt)
{
    {
        auto r = DB::globalRandomInt<int32_t>(0, 10);
        EXPECT_TRUE(r >= 0 && r <= 10);
    }

    {
        auto r = DB::globalRandomInt<int32_t>(-10, 0);
        EXPECT_TRUE(r >= -10 && r <= 0);
    }

    {
        auto r = DB::globalRandomInt<int32_t>(-10, 10);
        EXPECT_TRUE(r >= -10 && r <= 10);
    }

    {
        auto r = DB::globalRandomInt<int32_t>(-10, -5);
        EXPECT_TRUE(r >= -10 && r <= -5);
    }

    {
        auto r = DB::globalRandomInt<int32_t>(5, 10);
        EXPECT_TRUE(r >= 5 && r <= 10);
    }
}

TEST(RandomInt, UnsignedInt)
{
    {
        auto r = DB::globalRandomInt<uint32_t>(0, 10);
        EXPECT_TRUE(r >= 0 && r <= 10);
    }

    {
        auto r = DB::globalRandomInt<uint32_t>(5, 10);
        EXPECT_TRUE(r >= 5 && r <= 10);
    }
}
