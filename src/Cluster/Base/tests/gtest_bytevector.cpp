#include <Cluster/Base/ByteVector.h>

#include <base/defines.h>

#include <cstdint>
#include <cstring>
#include <new>

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

namespace
{
/// Force a genuine, failing allocation. A release build would otherwise elide the unused malloc/free
/// pair, assume the allocation succeeded, and drop the null-check (and its throw). The empty asm with
/// a memory clobber consumes the buffer so the optimizer must keep the real malloc; `reserved` stays
/// opaque behind the non-inlinable boundary so the call is not constant-folded.
/// Unused under the sanitizer builds below, where the only caller is compiled out.
[[maybe_unused]] [[gnu::noinline]] void allocateReserved(size_t reserved)
{
    cluster::ByteVector bytes(reserved);
    char * data = bytes.data();
    __asm__ __volatile__("" : : "r"(data) : "memory");
}
}

TEST(ByteVector, ThrowsBadAllocOnFailedReserve)
{
    /// A reservation that the allocator cannot satisfy must surface as std::bad_alloc rather than
    /// leaving the ByteVector with a null buffer that later NULL-derefs (e.g. on a torn-WAL read of
    /// a garbage size).
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
    /// These sanitizers replace the allocator and abort on the oversized malloc by default
    /// (allocator_may_return_null=0) instead of returning null, so the failure path is covered by the
    /// plain and UBSan builds rather than forcing a binary-wide allocator option. The ADDRESS_/
    /// THREAD_/MEMORY_SANITIZER macros come from <base/defines.h>, which must be included for the
    /// guard to fire.
    GTEST_SKIP() << "oversized malloc aborts under the sanitizer allocator; covered in plain builds";
#else
    EXPECT_THROW(allocateReserved(SIZE_MAX), std::bad_alloc);
#endif
}
