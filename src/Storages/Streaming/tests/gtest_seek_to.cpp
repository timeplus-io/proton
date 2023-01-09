#include <NativeLog/Record/Record.h>
#include <Storages/Streaming/SeekToInfo.h>
#include <base/ClockUtils.h>

#include <gtest/gtest.h>

TEST(ParseSeekTo, TimeBased)
{
    /// ISO8601
    {
        DB::SeekToInfo seek_to_info("2020-01-01T01:12:45Z");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], 1577841165000ll);
    }

    {
        DB::SeekToInfo seek_to_info("2020-01-01T01:12:45.123+08:00");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 2);
        ASSERT_EQ(timestamps[0], 1577812365123ll);
        ASSERT_EQ(timestamps[1], 1577812365123ll);
    }

    /// Relative time based
    {
        auto two_days_ago = DB::UTCMilliseconds::now() - 86400000 * 2;
        DB::SeekToInfo seek_to_info("-2d");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_days_ago);
        ASSERT_TRUE(timestamps[0] < two_days_ago + 500);
    }

    {
        auto two_hour_ago = DB::UTCMilliseconds::now() - 3600000 * 2;
        DB::SeekToInfo seek_to_info("-2h");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_hour_ago);
        ASSERT_TRUE(timestamps[0] < two_hour_ago + 500);
    }

    {
        auto two_second_ago = DB::UTCMilliseconds::now() - 1000 * 2;
        DB::SeekToInfo seek_to_info("-2s");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        /// It shall take less than 500 millisecond to parse seek to
        ASSERT_TRUE(timestamps[0] >= two_second_ago);
        ASSERT_TRUE(timestamps[0] < two_second_ago + 500);
    }

    {
        auto two_second_ago = DB::UTCMilliseconds::now() - 1000 * 2;
        DB::SeekToInfo seek_to_info("-2s");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_TRUE(seek_to_info.isTimeBased());
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
        DB::SeekToInfo seek_to_info("10");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], 10);
    }

    {
        DB::SeekToInfo seek_to_info("10");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, 10);
    }

    {
        DB::SeekToInfo seek_to_info("10,11");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 2);
        ASSERT_EQ(timestamps[0], 10);
        ASSERT_EQ(timestamps[1], 11);
    }
}

TEST(ParseSeekTo, Sepcial)
{
    {
        DB::SeekToInfo seek_to_info("");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::LATEST_SN);
    }

    {
        DB::SeekToInfo seek_to_info("");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, nlog::LATEST_SN);
    }

    {
        DB::SeekToInfo seek_to_info("latest");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::LATEST_SN);
    }

    {
        DB::SeekToInfo seek_to_info("latest,latest");
        seek_to_info.replicateForShards(2);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 2);
        for (auto timestamp : timestamps)
            ASSERT_EQ(timestamp, nlog::LATEST_SN);
    }

    {
        DB::SeekToInfo seek_to_info("earliest");
        seek_to_info.replicateForShards(1);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 1);
        ASSERT_EQ(timestamps[0], nlog::EARLIEST_SN);
    }

    {
        DB::SeekToInfo seek_to_info("earliest,13,latest");
        seek_to_info.replicateForShards(3);
        const auto & timestamps = seek_to_info.getSeekPoints();
        ASSERT_FALSE(seek_to_info.isTimeBased());
        ASSERT_EQ(timestamps.size(), 3);
        ASSERT_EQ(timestamps[0], nlog::EARLIEST_SN);
        ASSERT_EQ(timestamps[1], 13);
        ASSERT_EQ(timestamps[2], nlog::LATEST_SN);
    }
}

TEST(ParseSeekTo, TimeBasedBadCases)
{
    /// Relative time shall always negative
    ASSERT_ANY_THROW(DB::SeekToInfo("2h"));

    /// shards is less than number of seek to timestamps
    ASSERT_ANY_THROW(DB::SeekToInfo("2h,2h").replicateForShards(1));

    /// shards is greater than number of seek to timestamps
    ASSERT_ANY_THROW(DB::SeekToInfo("2h,2h").replicateForShards(3));
}

TEST(ParseSeekTo, SequenceBasedBadCases)
{
    /// Seek to sequence number shall always greater or equal to 0
    ASSERT_ANY_THROW(DB::SeekToInfo("-1").replicateForShards(1));

    /// shards is less than number of seek to sequences
    ASSERT_ANY_THROW(DB::SeekToInfo("2,2").replicateForShards(1));

    /// shards is greater than number of seek to sequences
    ASSERT_ANY_THROW(DB::SeekToInfo("2,2").replicateForShards(3));

    /// Mix sequences and timestamps
    ASSERT_ANY_THROW(DB::SeekToInfo("-2h,2").replicateForShards(3));
    ASSERT_ANY_THROW(DB::SeekToInfo("2020-01-01T01:12:45Z,2").replicateForShards(3));
}

TEST(ParseSeekTo, SpecialBadCases)
{
    ASSERT_ANY_THROW(DB::SeekToInfo(" ").replicateForShards(1));
    ASSERT_ANY_THROW(DB::SeekToInfo("xxx").replicateForShards(1));
}

