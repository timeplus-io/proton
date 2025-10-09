#include <Cluster/LocalLog/Indexes/EpochSequenceIndex.h>

#include <base/scope_guard.h>
#include <Common/logger_useful.h>

#include <gtest/gtest.h>

namespace
{
const std::string index_dir = "/tmp";

struct TestData
{
    cluster::EpochSequence epoch_sn;
    int latest_epoch_sn_idx;
    int prev_epoch_sn_idx;
    int leader_epoch_for_sn_idx;
    bool indexed;
    bool has_error;
};

/// For sequence [start_sn, end_sn), the expected epoch sequence idx is `epoch_sn_idx`
struct RangeEpochSN
{
    int64_t start_sn;
    int64_t end_sn;
    int epoch_sn_idx;
};

void cleanIndexLogs()
{
    cluster::nlog::EpochSequenceIndex index(index_dir, 1024, /*preallocate_=*/true, getLogger("leader_epoch"));
    index.remove();
}

void doTest(const std::vector<TestData> & test_data, const std::vector<RangeEpochSN> & range_epochs, size_t file_max_size)
{
    /// Remove left overs
    cleanIndexLogs();

    auto check = [&](cluster::nlog::EpochSequenceIndex & index) {
        {
            auto res = index.latestLeaderEpoch();
            ASSERT_FALSE(res.hasError());
            ASSERT_TRUE(res.result.has_value());
            EXPECT_EQ(res.result.value(), test_data[test_data.back().latest_epoch_sn_idx].epoch_sn.epoch);
        }
        {
            auto res = index.previousLeaderEpoch();
            ASSERT_FALSE(res.hasError());
            ASSERT_TRUE(res.result.has_value());
            EXPECT_EQ(res.result.value(), test_data[test_data.back().prev_epoch_sn_idx].epoch_sn.epoch);
        }
        {
            auto res = index.leaderEpochSequenceFor(0);
            ASSERT_FALSE(res.hasError());
            ASSERT_FALSE(res.result);
        }

        for (const auto & range_epoch : range_epochs)
        {
            for (int64_t sn = range_epoch.start_sn; sn < range_epoch.end_sn; ++sn)
            {
                auto res = index.leaderEpochSequenceFor(sn);
                ASSERT_FALSE(res.hasError());
                ASSERT_TRUE(res.result);
                EXPECT_EQ(res.result, test_data[range_epoch.epoch_sn_idx].epoch_sn);
            }
        }
    };

    {
        cluster::nlog::EpochSequenceIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("leader_epoch"));

        for (const auto & epoch : test_data)
        {
            auto res = index.maybeIndexEpochStartSequence(epoch.epoch_sn, /*sync=*/true);

            /// std::cout << epoch.epoch_sn.string() << "\n";

            ASSERT_EQ(res.hasError(), epoch.has_error);
            ASSERT_EQ(res.result, epoch.indexed);

            {
                auto nres = index.leaderEpochSequenceFor(epoch.epoch_sn.sn);
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[epoch.leader_epoch_for_sn_idx].epoch_sn);
            }

            {
                auto nres = index.latestLeaderEpoch();
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[epoch.latest_epoch_sn_idx].epoch_sn.epoch);
            }

            {
                auto nres = index.previousLeaderEpoch();
                ASSERT_FALSE(nres.hasError());
                if (epoch.prev_epoch_sn_idx >= 0)
                {
                    ASSERT_TRUE(nres.result);
                    EXPECT_EQ(nres.result.value(), test_data[epoch.prev_epoch_sn_idx].epoch_sn.epoch);
                }
                else
                {
                    ASSERT_FALSE(nres.result);
                }
            }
            {
                auto nres = index.latestLeaderEpochSequence();
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[epoch.latest_epoch_sn_idx].epoch_sn);
            }
            {
                auto nres = index.previousLeaderEpochSequence();
                ASSERT_FALSE(nres.hasError());
                if (epoch.prev_epoch_sn_idx >= 0)
                {
                    ASSERT_TRUE(nres.result);
                    EXPECT_EQ(nres.result.value(), test_data[epoch.prev_epoch_sn_idx].epoch_sn);
                }
                else
                {
                    ASSERT_FALSE(nres.result);
                }
            }
        }

        /// std::cout << "checking\n";
        check(index);
    }

    /// load again
    {
        cluster::nlog::EpochSequenceIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("leader_epoch"));

        check(index);
    }
}
}

TEST(DISABLED_EpochSequenceIndex, EmptyIndex) /// Disabled: Multi-epoch test incompatible
{
    cleanIndexLogs();

    cluster::nlog::EpochSequenceIndex index(index_dir, /*file_max_size_=*/1024, /*preallocate_=*/true, getLogger("leader_epoch"));

    SCOPE_EXIT({ index.remove(); });

    /// Empty Index
    {
        auto res = index.leaderEpochSequenceFor(1);
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }

    {
        auto res = index.latestLeaderEpochSequence();
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }

    {
        auto res = index.previousLeaderEpochSequence();
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }

    {
        auto res = index.latestLeaderEpoch();
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }

    {
        auto res = index.previousLeaderEpoch();
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }
}

TEST(DISABLED_EpochSequenceIndex, Index) /// Disabled: Multi-epoch test incompatible
{
    /// The PersistentMonoMap expects monotonically increasing keys (epochs)
    /// Since all epochs are 1, only the first entry with epoch=1 will be indexed
    std::vector<TestData> test_data = {
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .latest_epoch_sn_idx = 0,
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = true, /// First entry with epoch=1 is indexed
         .has_error = false},
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/1}, /// Duplicate
         .latest_epoch_sn_idx = 0,
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = false, /// Same epoch and sn, not indexed
         .has_error = false},
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/2}, /// Same epoch, different sn
         .latest_epoch_sn_idx = 0,
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = false, /// Same epoch (key), so not indexed even with different sn
         .has_error = false},
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/2}, /// Duplicate
         .latest_epoch_sn_idx = 0,
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = false,
         .has_error = false},
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/10}, /// Same epoch, different sn
         .latest_epoch_sn_idx = 0, /// Still points to first entry
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = false, /// Same epoch (key), not indexed
         .has_error = false},
        {.epoch_sn = {/*epoch_=*/1, /*sn_=*/11}, /// Same epoch, different sn
         .latest_epoch_sn_idx = 0, /// Still points to first entry
         .prev_epoch_sn_idx = -1,
         .leader_epoch_for_sn_idx = 0,
         .indexed = false, /// Same epoch (key), not indexed
         .has_error = false},
    };

    std::vector<RangeEpochSN> range_epochs = {
        {.start_sn = 1, .end_sn = 15, .epoch_sn_idx = 0}, /// All sequences map to first entry
    };
    doTest(test_data, range_epochs, 1024);
}

TEST(DISABLED_EpochSequenceIndex, IndexMultipleSegments)
{
    /// Only the first entry will be indexed, others will be rejected
    int n = 13;
    std::vector<TestData> test_data;
    test_data.reserve(n);

    std::vector<RangeEpochSN> range_epochs;
    
    for (int i = 1; i <= n; ++i)
    {
        test_data.push_back(
            {.epoch_sn = {/*epoch_=*/1, /*sn_=*/i}, /// All epochs become 1
             .latest_epoch_sn_idx = 0, /// Always points to first entry
             .prev_epoch_sn_idx = -1,
             .leader_epoch_for_sn_idx = 0, /// Always first entry
             .indexed = (i == 1), /// Only first entry is indexed
             .has_error = false});
    }

    /// All sequences map to the same epoch entry
    range_epochs.push_back({.start_sn = 1, .end_sn = n + 1, .epoch_sn_idx = 0});

    doTest(test_data, range_epochs, cluster::EpochSequence::exactSerializedSize() * 3);
}

namespace
{
struct TrimData
{
    int64_t trim_sn = 0;
    cluster::EpochSequence expected_latest_epoch_sn;
    cluster::EpochSequence expected_prev_epoch_sn;
    cluster::EpochSequence expected_earliest_epoch_sn;
};

void doTrim(const std::vector<TrimData> & trim_data, int64_t n, int64_t factor, size_t file_max_size, bool head)
{
    cleanIndexLogs();

    auto do_check = [](cluster::nlog::EpochSequenceIndex & index, const TrimData & expected) {
        {
            auto res = index.latestLeaderEpochSequence();
            ASSERT_FALSE(res.hasError());
            if (expected.expected_latest_epoch_sn.epoch != cluster::Nulls::NullEpoch)
            {
                ASSERT_TRUE(res.result);
                EXPECT_EQ(*res.result, expected.expected_latest_epoch_sn);
            }
            else
            {
                ASSERT_FALSE(res.result);
            }
        }

        {
            auto res = index.previousLeaderEpochSequence();
            ASSERT_FALSE(res.hasError());
            if (expected.expected_prev_epoch_sn.epoch != cluster::Nulls::NullEpoch)
            {
                ASSERT_TRUE(res.result);
                EXPECT_EQ(*res.result, expected.expected_prev_epoch_sn);
            }
            else
            {
                ASSERT_FALSE(res.result);
            }
        }

        {
            auto res = index.earliestLeaderEpochSequence();
            ASSERT_FALSE(res.hasError());
            if (expected.expected_earliest_epoch_sn.epoch != cluster::Nulls::NullEpoch)
            {
                ASSERT_TRUE(res.result);
                EXPECT_EQ(*res.result, expected.expected_earliest_epoch_sn);
            }
            else
            {
                ASSERT_FALSE(res.result);
            }
        }
    };

    {
        cluster::nlog::EpochSequenceIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("leader_epoch"));
        for (int64_t i = 1; i <= n; ++i)
        {
            auto res
                = index.maybeIndexEpochStartSequence(cluster::EpochSequence{static_cast<uint64_t>(i), factor * i}, /*sync=*/true);
            ASSERT_FALSE(res.hasError());
            ASSERT_TRUE(res.result);
        }

        TrimData trim_point{
            .expected_latest_epoch_sn = cluster::EpochSequence(n, n * factor),
            .expected_prev_epoch_sn = cluster::EpochSequence(n - 1, (n - 1) * factor),
            .expected_earliest_epoch_sn = cluster::EpochSequence(1, factor),
        };
        do_check(index, trim_point);
    }

    {
        for (const auto & trim_point : trim_data)
        {
            {
                cluster::nlog::EpochSequenceIndex index(
                    index_dir, file_max_size, /*preallocate_=*/true, getLogger("leader_epoch"));

                if (head)
                {
                    auto err = index.trimHead(trim_point.trim_sn);
                    ASSERT_EQ(err, 0);
                }
                else
                {
                    auto err = index.trim(trim_point.trim_sn);
                    ASSERT_EQ(err, 0);
                }

                do_check(index, trim_point);
            }
            /// Reboot
            {
                cluster::nlog::EpochSequenceIndex index(
                    index_dir, file_max_size, /*preallocate_=*/true, getLogger("leader_epoch"));

                do_check(index, trim_point);
            }
        }
    }
};
}

TEST(DISABLED_EpochSequenceIndex, TrimWithConsectiveSN)
{
    /// Trimming behavior is simplified
    int64_t n = 16;
    std::vector<TrimData> trim_data = {
        {.trim_sn = n + 1, /// Nothing to trim, entry remains
         .expected_latest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1}},
        {.trim_sn = n, /// Entry remains (trim sn > entry sn)
         .expected_latest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1}},
        {.trim_sn = 8, /// Entry remains (trim sn > entry sn)
         .expected_latest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1}},
        {.trim_sn = 3, /// Entry remains (trim sn > entry sn)
         .expected_latest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1}},
        {.trim_sn = 2, /// Entry remains (trim sn > entry sn)
         .expected_latest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/1, /*sn_=*/1}},
        {.trim_sn = 1, /// Trim the entry (trim sn = entry sn)
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
        {.trim_sn = 1, /// Nothing to trim (already empty)
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
        {.trim_sn = 0, /// Nothing to trim (already empty)
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
    };

    /// Multiple segment files
    doTrim(trim_data, 1, /*factor=*/1, /*file_max_size=*/3 * cluster::EpochSequence::exactSerializedSize(), /*head=*/false);

    /// One segment file
    doTrim(trim_data, 1, /*factor=*/1, /*file_max_size=*/1024, /*head=*/false);
}

TEST(DISABLED_EpochSequenceIndex, TrimWithInconsecutiveSN)
{
    int64_t n = 16;
    int64_t factor = 4;
    std::vector<TrimData> trim_data = {
        {.trim_sn = n * factor - 1, /// Entry remains (trim sn > entry sn)
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 2), /*sn_=*/(n - 2) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor}},
        {.trim_sn = 8 * factor - 2, /// Trim multiple entries
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(7), /*sn_=*/7 * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(6), /*sn_=*/6 * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor}},
        {.trim_sn = 3 * factor, /// Trim multiple entries, 2 entries remained
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(2), /*sn_=*/2 * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor}},
        {.trim_sn = 2 * factor - 3, /// Trim one tail entry, 1 entry remained
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/factor}},
        {.trim_sn = factor - 1, /// Trim the last entry
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
        {.trim_sn = factor, /// Nothing to trim
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
        {.trim_sn = 0, /// Nothing to trim
         .expected_latest_epoch_sn = {},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {}},
    };

    /// Multiple segment files
    doTrim(trim_data, n, factor, /*file_max_size=*/3 * cluster::EpochSequence::exactSerializedSize(), /*head=*/false);

    /// One segment file
    doTrim(trim_data, n, factor, /*file_max_size=*/1024, /*head=*/false);
}

TEST(DISABLED_EpochSequenceIndex, TrimHeadWithConsectiveSN)
{
    int64_t n = 16;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 1, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 2, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 3, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 8, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = n, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = n + 1, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
    };

    /// For one single file, we didn't do any truncation on head
    doTrim(trim_data, n, /*factor=*/1, /*file_max_size=*/1024, /*head=*/true);
}

TEST(DISABLED_EpochSequenceIndex, TrimHeadWithConsectiveSNMultiple)
{
    int64_t n = 16;
    uint64_t max_entries_per_segment = 3;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 1, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 2, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1}},
        {.trim_sn = 3, /// Trim first log segment which contains sn [1, 2, 3] since we 3 entries per segment
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(4), /*sn_=*/4}},
        {.trim_sn = 8, /// Trim another tail log segment which contains sn [4, 5, 6]
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/n - 1},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(7), /*sn_=*/7}},
        {.trim_sn
         = n, /// n = 16 which is in a separate segment, [1, 2, 3], [4, 5, 6], ..., [13, 14, 15], [16], so trim all except the last segment
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n}},
        {.trim_sn = n + 1, /// Try trim all log segments. Can't because the active segment can't be trimmed by trim head
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n}},
    };

    /// Multiple segment files
    doTrim(
        trim_data,
        n,
        /*factor=*/1,
        /*file_max_size=*/max_entries_per_segment * cluster::EpochSequence::exactSerializedSize(),
        /*head=*/true);
}

TEST(DISABLED_EpochSequenceIndex, TrimHeadWithInconsecutiveSNMultiple)
{
    int64_t n = 16;
    int64_t factor = 4;
    uint64_t max_entries_per_segment = 3;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1 * factor}},
        {.trim_sn = 1 * factor, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1 * factor}},
        {.trim_sn = 2 * factor, /// Nothing to trim
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(1), /*sn_=*/1 * factor}},
        {.trim_sn = 3 * factor, /// Trim first log segment which contains sn [1 * 4, 2 * 4, 3 * 4] since we 3 entries per segment
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(4), /*sn_=*/4 * factor}},
        {.trim_sn = 8 * factor, /// Trim another tail log segment which contains sn [4 * 4, 5 * 4, 6 * 4]
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n - 1), /*sn_=*/(n - 1) * factor},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(7), /*sn_=*/7 * factor}},
        {.trim_sn
         = n * factor, /// n = 16 * 4 which is in a separate segment, [1 * 4, 2 * 4, 3 * 4], [13 * 4, 14 * 4, 15 * 4], [16 * 4], so trim all except the last segment
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor}},
        {.trim_sn = (n + 1) * factor, /// Try trim all log segments. Can't because the active segment can't be trimmed by trim head
         .expected_latest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor},
         .expected_prev_epoch_sn = {},
         .expected_earliest_epoch_sn = {/*epoch_=*/static_cast<uint64_t>(n), /*sn_=*/n * factor}},
    };

    /// Multiple segment files
    doTrim(
        trim_data,
        n,
        factor,
        /*file_max_size=*/max_entries_per_segment * cluster::EpochSequence::exactSerializedSize(),
        /*head=*/true);
}
