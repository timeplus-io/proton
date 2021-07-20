#include <DistributedWriteAheadLog/ByteVector.h>

#include <gtest/gtest.h>

#include <string.h>

using namespace DWAL;
using namespace std;

TEST(ByteVector, Constructor)
{
    string str = "test constructor";
    ByteVector cache(str.length());

    char * addr = reinterpret_cast<char *>(cache.data());
    strcpy(addr, str.c_str());
    ByteVector move_cache(std::move(cache));

    EXPECT_EQ(cache.data(), nullptr);
    EXPECT_EQ(move_cache.capacity(), str.length());
    EXPECT_EQ(addr, reinterpret_cast<char *>(move_cache.data()));
    EXPECT_STREQ(reinterpret_cast<char *>(move_cache.data()), str.c_str());
}

TEST(ByteVector, Resize)
{
    string str = "test resize";
    ByteVector cache(str.length());

    /// str will swell to 2G
    for (int i = 0; i < 27; i++)
    {
        str += str;
        if (cache.capacity() <= str.length())
        {
            cache.resize(str.length());
        }
        EXPECT_NO_THROW(strcpy(reinterpret_cast<char *>(cache.data()), str.c_str()));
    }

    EXPECT_EQ(cache.size(), str.length());
    ASSERT_GT(cache.capacity(), cache.size());
}

TEST(ByteVector, Release)
{
    string str = "test release";
    ByteVector cache(str.length());

    char * addr = reinterpret_cast<char *>(cache.data());
    strcpy(addr, str.c_str());
    cache.release();

    EXPECT_EQ(cache.data(), nullptr);
    EXPECT_EQ(cache.size(), 0);
}
