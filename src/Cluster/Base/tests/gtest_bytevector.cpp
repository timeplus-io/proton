#include <Cluster/Base/ByteVector.h>

#include <cstring>

#include <gtest/gtest.h>

TEST(ByteVector, Constructor)
{
    std::string str = "test constructor";
    cluster::ByteVector cache(str.length() + 1);

    char * addr = reinterpret_cast<char *>(cache.data());
    strcpy(addr, str.c_str());
    cluster::ByteVector move_cache(std::move(cache));

    EXPECT_EQ(cache.data(), nullptr);
    EXPECT_EQ(move_cache.capacity(), str.length() + 1);
    EXPECT_EQ(addr, move_cache.data());
    EXPECT_STREQ(move_cache.data(), str.c_str());
}

TEST(ByteVector, EmptyData)
{
    cluster::ByteVector bytes(std::string_view{});
    ASSERT_EQ(bytes.size(), 0);
    ASSERT_TRUE(bytes.empty());
    ASSERT_TRUE(bytes.stringView().empty());
    ASSERT_TRUE(bytes.string().empty());
}

TEST(ByteVector, CloneEmptyData)
{
    cluster::ByteVector bytes(std::string_view{});
    auto cloned = bytes.clone();

    ASSERT_EQ(cloned.size(), 0);
    ASSERT_TRUE(cloned.empty());
    ASSERT_TRUE(cloned.stringView().empty());
    ASSERT_TRUE(cloned.string().empty());
}

TEST(ByteVector, Resize)
{
    std::string str = "test resize";
    cluster::ByteVector cache(str.length());

    /// str will swell to 2G
    for (int i = 0; i < 27; i++)
    {
        str += str;
        if (cache.capacity() <= str.length())
            cache.resize(str.length() + 1);

        EXPECT_NO_THROW(strcpy(cache.data(), str.c_str()));
    }

    EXPECT_EQ(cache.size(), str.length() + 1);
    EXPECT_GE(cache.capacity(), cache.size());
}

TEST(ByteVector, Release)
{
    std::string str = "test release";
    cluster::ByteVector cache(str.length() + 1);

    char * addr = cache.data();
    strcpy(addr, str.c_str());
    cache.release();

    EXPECT_EQ(cache.data(), nullptr);
    EXPECT_EQ(cache.size(), 0);
}
