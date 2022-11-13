#include <NativeLog/Record/Record.h>
#include <Storages/Streaming/storageUtil.h>
#include <base/ClockUtils.h>

#include <gtest/gtest.h>

TEST(ParseSeekTo, TimeBased)
{
    /// ISO8601
    {
        auto [time_based, timestamps] = DB::parseSeekTo("2020-01-01T01:12:45Z", 1, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], 1577841165000ll);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("2020-01-01T01:12:45.123+08:00", 2, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        ASSERT_EQ(timestamps[0], 1577812365123ll);
        ASSERT_EQ(timestamps[1], 1577812365123ll);
    }

    /// Relative time based
    {
        auto two_days_ago = DB::UTCMilliseconds::now() - 86400000 * 2;
        auto [time_based, timestamps] = DB::parseSeekTo("-2d", 1, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_days_ago);
        ASSERT_TRUE(timestamps[0] < two_days_ago + 500);
    }

    {
        auto two_hour_ago = DB::UTCMilliseconds::now() - 3600000 * 2;
        auto [time_based, timestamps] = DB::parseSeekTo("-2h", 1, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_hour_ago);
        ASSERT_TRUE(timestamps[0] < two_hour_ago + 500);
    }

    {
        auto two_second_ago = DB::UTCMilliseconds::now() - 1000 * 2;
        auto [time_based, timestamps] = DB::parseSeekTo("-2s", 1, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_second_ago);
        ASSERT_TRUE(timestamps[0] < two_second_ago + 500);
    }

    {
        auto two_second_ago = DB::UTCMilliseconds::now() - 1000 * 2;
        auto [time_based, timestamps] = DB::parseSeekTo("-2s", 2, true);
        ASSERT_TRUE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        /// It shall take less than 500 millisecond to parse seek to
        for (auto timestamp : timestamps)
        {
            ASSERT_TRUE(timestamp >= two_second_ago);
            ASSERT_TRUE(timestamp < two_second_ago + 500);
        }
    }
}

TEST(ParseSeekTo, SequenceBased)
{
    {
        auto [time_based, timestamps] = DB::parseSeekTo("10", 1, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], 10);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("10", 2, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, 10);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("10,11", 2, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        ASSERT_EQ(timestamps[0], 10);
        ASSERT_EQ(timestamps[1], 11);
    }
}

TEST(ParseSeekTo, Sepcial)
{
    {
        auto [time_based, timestamps] = DB::parseSeekTo("", 1, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::LATEST_SN);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("", 2, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, nlog::LATEST_SN);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("latest", 1, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::LATEST_SN);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("latest,latest", 2, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, nlog::LATEST_SN);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("earliest", 1, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::EARLIEST_SN);
    }

    {
        auto [time_based, timestamps] = DB::parseSeekTo("earliest,13,latest", 3, true);
        ASSERT_FALSE(time_based);
        ASSERT_EQ(timestamps.size(), 3);
        ASSERT_EQ(timestamps[0], nlog::EARLIEST_SN);
        ASSERT_EQ(timestamps[1], 13);
        ASSERT_EQ(timestamps[2], nlog::LATEST_SN);
    }
}

TEST(ParseSeekTo, TimeBasedBadCases)
{
    /// Relative time shall always negative
    ASSERT_ANY_THROW(DB::parseSeekTo("2h", 1, true));

    /// shards is less than number of seek to timestamps
    ASSERT_ANY_THROW(DB::parseSeekTo("2h,2h", 1, true));

    /// shards is greater than number of seek to timestamps
    ASSERT_ANY_THROW(DB::parseSeekTo("2h,2h", 3, true));
}

TEST(ParseSeekTo, SequenceBasedBadCases)
{
    /// Seek to sequence number shall always greater or equal to 0
    ASSERT_ANY_THROW(DB::parseSeekTo("-1", 1, true));

    /// shards is less than number of seek to sequences
    ASSERT_ANY_THROW(DB::parseSeekTo("2,2", 1, true));

    /// shards is greater than number of seek to sequences
    ASSERT_ANY_THROW(DB::parseSeekTo("2,2", 3, true));

    /// Mix sequences and timestamps
    ASSERT_ANY_THROW(DB::parseSeekTo("-2h,2", 3, true));
    ASSERT_ANY_THROW(DB::parseSeekTo("2020-01-01T01:12:45Z,2", 3, true));
}

TEST(ParseSeekTo, SpecialBadCases)
{
    ASSERT_ANY_THROW(DB::parseSeekTo(" ", 1, true));
    ASSERT_ANY_THROW(DB::parseSeekTo("xxx", 1, true));
}

