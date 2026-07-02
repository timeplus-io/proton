#include <gtest/gtest.h>

#include <Common/LogstoreRetentionSettings.h>

#include <limits>

namespace
{
using namespace DB;
}

GTEST_TEST(LogstoreRetentionSettings, EncodeForMetastore)
{
    EXPECT_EQ(encodeLogstoreRetentionForMetastore(Int64(0)), kLogstoreRetentionInheritDefaults);
    EXPECT_EQ(encodeLogstoreRetentionForMetastore(Int64(-1)), kLogstoreRetentionNoLimit);
    EXPECT_EQ(encodeLogstoreRetentionForMetastore(Int64(-123)), kLogstoreRetentionNoLimit);
    EXPECT_EQ(encodeLogstoreRetentionForMetastore(Int64(1)), UInt64(1));
}

GTEST_TEST(LogstoreRetentionSettings, DecodeForRuntime)
{
    EXPECT_EQ(decodeLogstoreRetentionForRuntime(UInt64(0)), Int64(0));
    EXPECT_EQ(decodeLogstoreRetentionForRuntime(kLogstoreRetentionNoLimit), Int64(-1));
    EXPECT_EQ(decodeLogstoreRetentionForRuntime(UInt64(1)), Int64(1));

    constexpr auto int64_max_as_u64 = static_cast<UInt64>(std::numeric_limits<Int64>::max());
    EXPECT_EQ(decodeLogstoreRetentionForRuntime(int64_max_as_u64), std::numeric_limits<Int64>::max());
    EXPECT_EQ(decodeLogstoreRetentionForRuntime(int64_max_as_u64 + 1), std::numeric_limits<Int64>::max());
}
