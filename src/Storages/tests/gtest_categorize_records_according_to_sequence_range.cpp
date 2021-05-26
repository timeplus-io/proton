#include <gtest/gtest.h>

#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Storages/DistributedMergeTreeCallbackData.h>
#include <Storages/MergeTree/SequenceInfo.h>


TEST(CategorizeRecords, SequenceRanges)
{
    /// Build Sequence Ranges
    /// [2, 3]; [4, 9] committed; [10, 13]; [14, 18] committed; [19, 23] parts : 0, 1, 3 committed
    /// [24, 37] committed; [38, 43] parts : 0, 2, 4 committed; [44, 47] committed; [48, 49] new records
    DB::SequenceRanges sequence_ranges;
    sequence_ranges.emplace_back(Int64(2), Int64(3));
    sequence_ranges.emplace_back(Int64(10), Int64(13));
    sequence_ranges.emplace_back(19, 23, 2, 4);
    sequence_ranges.emplace_back(38, 43, 1, 5);
    sequence_ranges.emplace_back(38, 43, 3, 5);

    /// Build records
    DB::IDistributedWriteAheadLog::RecordPtrs records;

    for (Int64 i = 2; i < 50; ++i)
    {
        records.push_back(std::make_shared<DB::IDistributedWriteAheadLog::Record>(i));
    }

    auto range_buckets = DB::DistributedMergeTreeCallbackData::categorizeRecordsAccordingToSequenceRanges(records, sequence_ranges, 47);
    EXPECT_EQ(range_buckets.size(), 5);
    EXPECT_EQ(range_buckets[0].first.size(), 2);
    EXPECT_EQ(range_buckets[0].first[0]->sn, 2);
    EXPECT_EQ(range_buckets[0].first[1]->sn, 3);
    EXPECT_EQ(range_buckets[0].second.size(), 1);
    EXPECT_EQ(range_buckets[0].second[0], (DB::SequenceRange{Int64(2), Int64(3)}));

    EXPECT_EQ(range_buckets[1].first.size(), 4);
    EXPECT_EQ(range_buckets[1].first[0]->sn, 10);
    EXPECT_EQ(range_buckets[1].first[1]->sn, 11);
    EXPECT_EQ(range_buckets[1].first[2]->sn, 12);
    EXPECT_EQ(range_buckets[1].first[3]->sn, 13);
    EXPECT_EQ(range_buckets[1].second.size(), 1);
    EXPECT_EQ(range_buckets[1].second[0], (DB::SequenceRange{Int64(10), Int64(13)}));

    EXPECT_EQ(range_buckets[2].first.size(), 5);
    EXPECT_EQ(range_buckets[2].first[0]->sn, 19);
    EXPECT_EQ(range_buckets[2].first[1]->sn, 20);
    EXPECT_EQ(range_buckets[2].first[2]->sn, 21);
    EXPECT_EQ(range_buckets[2].first[3]->sn, 22);
    EXPECT_EQ(range_buckets[2].first[4]->sn, 23);
    EXPECT_EQ(range_buckets[2].second.size(), 1);
    EXPECT_EQ(range_buckets[2].second[0], (DB::SequenceRange{19, 23, 2, 4}));

    EXPECT_EQ(range_buckets[3].first.size(), 6);
    EXPECT_EQ(range_buckets[3].first[0]->sn, 38);
    EXPECT_EQ(range_buckets[3].first[1]->sn, 39);
    EXPECT_EQ(range_buckets[3].first[2]->sn, 40);
    EXPECT_EQ(range_buckets[3].first[3]->sn, 41);
    EXPECT_EQ(range_buckets[3].first[4]->sn, 42);
    EXPECT_EQ(range_buckets[3].first[5]->sn, 43);
    EXPECT_EQ(range_buckets[3].second.size(), 2);
    EXPECT_EQ(range_buckets[3].second[0], (DB::SequenceRange{38, 43, 1, 5}));
    EXPECT_EQ(range_buckets[3].second[1], (DB::SequenceRange{38, 43, 3, 5}));

    EXPECT_EQ(range_buckets[4].first.size(), 2);
    EXPECT_EQ(range_buckets[4].first[0]->sn, 48);
    EXPECT_EQ(range_buckets[4].first[1]->sn, 49);
    EXPECT_TRUE(range_buckets[4].second.empty());
}
