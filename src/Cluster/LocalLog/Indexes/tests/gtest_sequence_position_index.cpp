#include <Cluster/LocalLog/Indexes/SequencePositionIndex.h>

#include <base/scope_guard.h>
#include <Common/logger_useful.h>

#include <gtest/gtest.h>

namespace
{
const std::string index_dir = "/tmp";

struct TestData
{
    cluster::nlog::SequencePosition sn_pos;
    int last_sn_pos_idx;
    int left_lower_bound_for_pos_idx;
    int lower_bound_for_pos_idx;
    bool indexed;
    bool has_error;
};

struct ValuesAfterReboot
{
    cluster::nlog::SequencePosition last_indexed;

    struct LeftLowerBoundSequenceExpected
    {
        int64_t sn;
        cluster::nlog::SequencePosition expected;
    };
    std::vector<LeftLowerBoundSequenceExpected> left_lower_bound_sn_expects;

    struct LowerBoundPositionExpected
    {
        uint64_t file_position;
        cluster::nlog::SequencePosition expected;
    };
    std::vector<LowerBoundPositionExpected> lower_bound_pos_expects;
};

void cleanIndexLogs()
{
    cluster::nlog::SequencePositionIndex index(index_dir, 1024, /*preallocate_=*/true, getLogger("sn_pos"));
    index.remove();
}

void checkValuesAfterBoot(cluster::nlog::SequencePositionIndex & index, const ValuesAfterReboot & values_after_reboot)
{
    if (values_after_reboot.last_indexed.sn != 0)
    {
        auto res = index.lastIndexedSequencePosition();
        ASSERT_FALSE(res.hasError());
        ASSERT_TRUE(res.result);
        EXPECT_EQ(*res.result, values_after_reboot.last_indexed);
    }

    for (const auto & left_lower : values_after_reboot.left_lower_bound_sn_expects)
    {
        auto res = index.leftLowerBoundPositionForSequence(left_lower.sn);
        ASSERT_FALSE(res.hasError());

        if (left_lower.expected.sn != 0)
        {
            ASSERT_TRUE(res.result);
            EXPECT_EQ(*res.result, left_lower.expected);
        }
    }

    for (const auto & lower_bound : values_after_reboot.lower_bound_pos_expects)
    {
        auto res = index.lowerBoundSequenceForPosition(lower_bound.file_position);
        ASSERT_FALSE(res.hasError());

        if (lower_bound.expected.sn != 0)
        {
            ASSERT_TRUE(res.result);
            EXPECT_EQ(*res.result, lower_bound.expected);
        }
    }
}

void doTest(const std::vector<TestData> & test_data, const ValuesAfterReboot & values_after_reboot, size_t file_max_size)
{
    /// Remove left overs
    cleanIndexLogs();

    {
        cluster::nlog::SequencePositionIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("sn_pos"));

        {
            auto res = index.leftLowerBoundPositionForSequence(0);
            ASSERT_FALSE(res.hasError());
            ASSERT_FALSE(res.result);
        }

        for (const auto & sn_pos : test_data)
        {
            auto res = index.maybeIndex(sn_pos.sn_pos, /*sync=*/true);

            /// std::cout << sn_pos.sn_pos.string() << "\n";

            ASSERT_EQ(res.hasError(), sn_pos.has_error);
            ASSERT_EQ(res.result, sn_pos.indexed);

            {
                auto nres = index.lastIndexedSequencePosition();
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[sn_pos.last_sn_pos_idx].sn_pos);
            }

            if (sn_pos.left_lower_bound_for_pos_idx >= 0)
            {
                auto nres = index.leftLowerBoundPositionForSequence(sn_pos.sn_pos.sn);
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[sn_pos.left_lower_bound_for_pos_idx].sn_pos);
            }

            if (sn_pos.indexed)
            {
                auto nres = index.lowerBoundSequenceForPosition(sn_pos.sn_pos.file_position);
                ASSERT_FALSE(nres.hasError());
                ASSERT_TRUE(nres.result);
                EXPECT_EQ(nres.result.value(), test_data[sn_pos.lower_bound_for_pos_idx].sn_pos);
            }
        }
    }

    /// load again
    {
        cluster::nlog::SequencePositionIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("sn_pos"));

        checkValuesAfterBoot(index, values_after_reboot);
    }
}
}

TEST(SequencePositionIndex, EmptyIndex)
{
    cleanIndexLogs();

    cluster::nlog::SequencePositionIndex index(index_dir, /*file_max_size_=*/1024, /*preallocate_=*/true, getLogger("sn_pos"));

    SCOPE_EXIT({ index.remove(); });

    /// Empty Index
    {
        auto res = index.leftLowerBoundPositionForSequence(1);
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }
    {
        auto res = index.lastIndexedSequencePosition();
        ASSERT_FALSE(res.hasError());
        ASSERT_FALSE(res.result);
    }
}

TEST(SequencePositionIndex, Index)
{
    std::vector<TestData> test_data = {
        {.sn_pos = {/*file_position_=*/2, /*file_position_=*/1},
         .last_sn_pos_idx = 0,
         .left_lower_bound_for_pos_idx = 0,
         .lower_bound_for_pos_idx = 0,
         .indexed = true,
         .has_error = false},
        {.sn_pos = {/*file_position_=*/2, /*file_position_=*/1},
         .last_sn_pos_idx = 0,
         .left_lower_bound_for_pos_idx = 0,
         .lower_bound_for_pos_idx = 0,
         .indexed = false,
         .has_error = false},
        {.sn_pos = {/*file_position_=*/2, /*file_position_=*/2},
         .last_sn_pos_idx = 0,
         .left_lower_bound_for_pos_idx = 0,
         .lower_bound_for_pos_idx = 0,
         .indexed = false,
         .has_error = false},
        {.sn_pos = {/*file_position_=*/1, /*file_position_=*/2},
         .last_sn_pos_idx = 0,
         .left_lower_bound_for_pos_idx = -1,
         .lower_bound_for_pos_idx = 0,
         .indexed = false,
         .has_error = false}, /// Not appended
        {.sn_pos = {/*file_position_=*/3, /*file_position_=*/10},
         .last_sn_pos_idx = 4,
         .left_lower_bound_for_pos_idx = 4,
         .lower_bound_for_pos_idx = 4,
         .indexed = true,
         .has_error = false},
        {.sn_pos = {/*file_position_=*/3, /*file_position_=*/11},
         .last_sn_pos_idx = 4,
         .left_lower_bound_for_pos_idx = 4,
         .lower_bound_for_pos_idx = 4,
         .indexed = false,
         .has_error = false},
    };

    ValuesAfterReboot values_after_reboot = {
        .last_indexed = {/*file_position_=*/3, /*file_position_=*/10},
        .left_lower_bound_sn_expects = {
            {.sn = 1, .expected = {}},
            {.sn = 2, .expected = {/*file_position_=*/2, /*file_position_=*/1}},
            {.sn = 3, .expected = {/*file_position_=*/3, /*file_position_=*/10}},
        },
        .lower_bound_pos_expects = {
            {.file_position = 1, .expected = {/*file_position_=*/2, /*file_position_=*/1}},
            {.file_position = 2, .expected = {/*file_position_=*/3, /*file_position_=*/10}},
            {.file_position = 3, .expected = {/*file_position_=*/3, /*file_position_=*/10}},
            {.file_position = 10, .expected = {/*file_position_=*/3, /*file_position_=*/10}},
            {.file_position = 11, .expected = {}},
        }
    };

    doTest(test_data, values_after_reboot, 1024);
}

TEST(SequencePositionIndex, IndexMultipleSegments)
{
    int n = 13;
    std::vector<TestData> test_data;
    test_data.reserve(n);

    for (int i = 1; i <= n; ++i)
    {
        test_data.push_back(
            {.sn_pos = {/*file_position_=*/i, /*file_position_=*/static_cast<uint64_t>(i)},
             .last_sn_pos_idx = i - 1,
             .left_lower_bound_for_pos_idx = i - 1,
             .lower_bound_for_pos_idx = i - 1,
             .indexed = true,
             .has_error = false});
    }

    ValuesAfterReboot values_after_reboot = {
        .last_indexed = {/*file_position_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
        .left_lower_bound_sn_expects = {
            {.sn = 0, .expected = {}},
            {.sn = 1, .expected = {/*file_position_=*/1, /*file_position_=*/1}},
            {.sn = 2, .expected = {/*file_position_=*/2, /*file_position_=*/2}},
            {.sn = n - 1, .expected = {/*file_position_=*/n-1, /*file_position_=*/static_cast<uint64_t>(n-1)}},
            {.sn = n, .expected = {/*file_position_=*/n, /*file_position_=*/static_cast<uint64_t>(n)}},
        },
        .lower_bound_pos_expects = {
            {.file_position = 0, .expected = {}},
            {.file_position = 1, .expected = {/*file_position_=*/1, /*file_position_=*/1}},
            {.file_position = 2, .expected = {/*file_position_=*/2, /*file_position_=*/2}},
            {.file_position = 3, .expected = {/*file_position_=*/3, /*file_position_=*/3}},
            {.file_position = static_cast<uint64_t>(n-1), .expected = {/*file_position_=*/n - 1, /*file_position_=*/static_cast<uint64_t>(n-1)}},
            {.file_position = static_cast<uint64_t>(n), .expected = {/*file_position_=*/n, /*file_position_=*/static_cast<uint64_t>(n)}},
        }
    };

    doTest(test_data, values_after_reboot, cluster::nlog::SequencePosition::exactSerializedSize() * 3);
}

namespace
{
struct TrimData
{
    int64_t trim_sn = 0;
    cluster::nlog::SequencePosition expected_last_sn_pos;
    cluster::nlog::SequencePosition expected_first_sn_pos = {};
};

void doTrim(const std::vector<TrimData> & trim_data, int64_t n, int64_t factor, size_t file_max_size, bool head)
{
    cleanIndexLogs();

    auto do_check = [](cluster::nlog::SequencePositionIndex & index, const TrimData & expected) {
        {
            auto res = index.lastIndexedSequencePosition();
            ASSERT_FALSE(res.hasError());
            if (expected.expected_last_sn_pos.sn != 0)
            {
                ASSERT_TRUE(res.result);
                EXPECT_EQ(*res.result, expected.expected_last_sn_pos);
            }
            else
            {
                ASSERT_FALSE(res.result);
            }
        }
    };

    {
        cluster::nlog::SequencePositionIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("sn_pos"));
        for (int64_t i = 1; i <= n; ++i)
        {
            auto res = index.maybeIndex(cluster::nlog::SequencePosition{factor * i, static_cast<uint64_t>(i)}, /*sync=*/true);
            ASSERT_FALSE(res.hasError());
            ASSERT_TRUE(res.result);
        }

        TrimData trim_point{
            .expected_last_sn_pos = cluster::nlog::SequencePosition{n * factor, static_cast<uint64_t>(n)},
        };
        do_check(index, trim_point);
    }

    {
        for (const auto & trim_point : trim_data)
        {
            {
                cluster::nlog::SequencePositionIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("sn_pos"));

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
                cluster::nlog::SequencePositionIndex index(index_dir, file_max_size, /*preallocate_=*/true, getLogger("sn_pos"));

                do_check(index, trim_point);
            }
        }
    }
};
}

TEST(SequencePositionIndex, TrimWithConsectiveSN)
{
    int64_t n = 16;
    std::vector<TrimData> trim_data = {
        {.trim_sn = n + 1, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = n, /// Trim the tail entry
         .expected_last_sn_pos = {/*sn_=*/n - 1, /*file_position_=*/static_cast<uint64_t>(n - 1)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 8, /// Trim multiple entries
         .expected_last_sn_pos = {/*sn_=*/7, /*file_position_=*/static_cast<uint64_t>(7)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 3, /// Trim multiple entries, 2 entries remained
         .expected_last_sn_pos = {/*sn_=*/2, /*file_position_=*/static_cast<uint64_t>(2)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 2, /// Trim one tail entry, 1 entry remained
         .expected_last_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 0, /// Trim the last entry
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
        {.trim_sn = 1, /// Nothing to trim
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
        {.trim_sn = 0, /// Nothing to trim
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
    };

    /// Multiple segment files
    doTrim(trim_data, n, /*factor=*/1, /*file_max_size=*/3 * cluster::nlog::SequencePosition::exactSerializedSize(), /*head=*/false);

    /// One segment file
    doTrim(trim_data, n, /*factor=*/1, /*file_max_size=*/1024, /*head=*/false);
}

TEST(SequencePositionIndex, TrimWithInconsecutiveSN)
{
    int64_t n = 16;
    int64_t factor = 4;
    std::vector<TrimData> trim_data = {
        {.trim_sn = n * factor - 1, /// Trim the tail entry
         .expected_last_sn_pos = {/*sn_=*/(n - 1) * factor, /*file_position_=*/static_cast<uint64_t>(n - 1)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 8 * factor - 2, /// Trim multiple entries
         .expected_last_sn_pos = {/*sn_=*/7 * factor, /*file_position_=*/static_cast<uint64_t>(7)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 3 * factor, /// Trim multiple entries, 2 entries remained
         .expected_last_sn_pos = {/*sn_=*/2 * factor, /*file_position_=*/static_cast<uint64_t>(2)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = 2 * factor - 3, /// Trim one tail entry, 1 entry remained
         .expected_last_sn_pos = {/*sn_=*/factor, /*file_position_=*/static_cast<uint64_t>(1)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/static_cast<uint64_t>(1)}},
        {.trim_sn = factor - 1, /// Trim the last entry
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
        {.trim_sn = factor, /// Nothing to trim
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
        {.trim_sn = 0, /// Nothing to trim
         .expected_last_sn_pos = {},
         .expected_first_sn_pos = {}},
    };

    /// Multiple segment files
    doTrim(trim_data, n, factor, /*file_max_size=*/3 * cluster::nlog::SequencePosition::exactSerializedSize(), /*head=*/false);

    /// One segment file
    doTrim(trim_data, n, factor, /*file_max_size=*/1024, /*head=*/false);
}

TEST(SequencePositionIndex, TrimHeadWithConsectiveSN)
{
    int64_t n = 16;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 1, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 2, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 3, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 8, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = n, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = n + 1, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
    };

    /// For one single file, we didn't do any truncation on head
    doTrim(trim_data, n, /*factor=*/1, /*file_max_size=*/1024, /*head=*/true);
}

TEST(SequencePositionIndex, TrimHeadWithConsectiveSNMultiple)
{
    int64_t n = 16;
    uint64_t max_entries_per_segment = 3;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 1, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 2, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1, /*file_position_=*/1}},
        {.trim_sn = 3, /// Trim first log segment which contains sn [1, 2, 3] since we 3 entries per segment
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/4, /*file_position_=*/4}},
        {.trim_sn = 8, /// Trim another tail log segment which contains sn [4, 5, 6]
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/7, /*file_position_=*/7}},
        {.trim_sn
         = n, /// n = 16 which is in a separate segment, [1, 2, 3], [4, 5, 6], ..., [13, 14, 15], [16], so trim all except the last segment
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)}},
        {.trim_sn = n + 1, /// Try trim all log segments. Can't because the active segment can't be trimmed by trim head
         .expected_last_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/n, /*file_position_=*/static_cast<uint64_t>(n)}},
    };

    /// Multiple segment files
    doTrim(
        trim_data,
        n,
        /*factor=*/1,
        /*file_max_size=*/max_entries_per_segment * cluster::nlog::SequencePosition::exactSerializedSize(),
        /*head=*/true);
}

TEST(SequencePositionIndex, TrimHeadWithInconsecutiveSNMultiple)
{
    int64_t n = 16;
    int64_t factor = 4;
    uint64_t max_entries_per_segment = 3;
    std::vector<TrimData> trim_data = {
        {.trim_sn = 0, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1 * factor, /*file_position_=*/1}},
        {.trim_sn = 1 * factor, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1 * factor, /*file_position_=*/1}},
        {.trim_sn = 2 * factor, /// Nothing to trim
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/1 * factor, /*file_position_=*/1}},
        {.trim_sn = 3 * factor, /// Trim first log segment which contains sn [1 * 4, 2 * 4, 3 * 4] since we 3 entries per segment
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/4 * factor, /*file_position_=*/4}},
        {.trim_sn = 8 * factor, /// Trim another tail log segment which contains sn [4 * 4, 5 * 4, 6 * 4]
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/7 * factor, /*file_position_=*/7}},
        {.trim_sn
         = n * factor, /// n = 16 * 4 which is in a separate segment, [1 * 4, 2 * 4, 3 * 4], [13 * 4, 14 * 4, 15 * 4], [16 * 4], so trim all except the last segment
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/n * factor, /*file_position=*/static_cast<uint64_t>(n)}},
        {.trim_sn = (n + 1) * factor, /// Try trim all log segments. Can't because the active segment can't be trimmed by trim head
         .expected_last_sn_pos = {/*sn_=*/n * factor, /*file_position_=*/static_cast<uint64_t>(n)},
         .expected_first_sn_pos = {/*sn_=*/n * factor, /*file_position=*/static_cast<uint64_t>(n)}},
    };

    /// Multiple segment files
    doTrim(
        trim_data,
        n,
        factor,
        /*file_max_size=*/max_entries_per_segment * cluster::nlog::SequencePosition::exactSerializedSize(),
        /*head=*/true);
}
