#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>

#include <gtest/gtest.h>

TEST(Serde, VarUInt)
{
    struct Test
    {
        uint64_t i;
        uint64_t e;
    };

    std::vector<Test> tests = {
        {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()},
        {std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min()},
        {std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()},
        {std::numeric_limits<int64_t>::max() - 1, std::numeric_limits<int64_t>::max() - 1},
        {static_cast<uint64_t>(2ull | 0x80'00'00'00'00'00'00'00ull), static_cast<uint64_t>(2ull | 0x80'00'00'00'00'00'00'00ull)},
        {static_cast<uint64_t>(2ull | 0x40'00'00'00'00'00'00'00ull), static_cast<uint64_t>(2ull | 0x40'00'00'00'00'00'00'00ull)},
    };

    for (const auto & t : tests)
    {
        std::vector<char> v;
        v.reserve(sizeof(t.i));

        {
            DB::WriteBufferFromVector<std::vector<char>> wb(v);
            DB::writeVarUInt(t.i, wb);
        }
        {
            DB::ReadBufferFromMemory rb(v.data(), v.size());

            uint64_t r = 0;
            DB::readVarUInt(r, rb);
            ASSERT_EQ(r, t.e);
        }
    }
}

TEST(Serde, VarInt)
{
    struct Test
    {
        int64_t i;
        int64_t e;
    };

    std::vector<Test> tests = {
        {-1, -1},
        {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()},
        {std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()},
    };

    for (const auto & t : tests)
    {
        std::vector<char> v;
        v.reserve(sizeof(t.i));

        {
            DB::WriteBufferFromVector<std::vector<char>> wb(v);
            DB::writeVarInt(t.i, wb);
        }
        {
            DB::ReadBufferFromMemory rb(v.data(), v.size());

            int64_t r = 0;
            DB::readVarInt(r, rb);
            ASSERT_EQ(r, t.e);
        }
    }
}

TEST(Serde, IntBinary)
{
    struct Test
    {
        int64_t i;
        int64_t e;
    };

    std::vector<Test> tests = {
        {std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()},
        {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()},
    };

    for (const auto & t : tests)
    {
        std::vector<char> v;
        v.reserve(sizeof(t.i));

        {
            DB::WriteBufferFromVector<std::vector<char>> wb(v);
            DB::writeIntBinary(t.i, wb);
        }
        {
            DB::ReadBufferFromMemory rb(v.data(), v.size());

            int64_t r = 0;
            DB::readIntBinary(r, rb);
            ASSERT_EQ(r, t.e);
        }
    }
}

TEST(Serde, UIntBinary)
{
    struct Test
    {
        uint64_t i;
        uint64_t e;
    };

    std::vector<Test> tests = {
        {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()},
        {std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min()},
    };

    for (const auto & t : tests)
    {
        std::vector<char> v;
        v.reserve(sizeof(t.i));

        {
            DB::WriteBufferFromVector<std::vector<char>> wb(v);
            DB::writeIntBinary(t.i, wb);
        }
        {
            DB::ReadBufferFromMemory rb(v.data(), v.size());

            uint64_t r = 0;
            DB::readIntBinary(r, rb);
            ASSERT_EQ(r, t.e);
        }
    }
}
