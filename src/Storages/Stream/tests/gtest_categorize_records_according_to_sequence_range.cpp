#include <gtest/gtest.h>

#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/SequenceInfo.h>
#include <Storages/Stream/StreamCallbackData.h>

#include <algorithm>
#include <limits>
#include <map>


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
    cluster::SchemaRecordPtrs records;

    for (Int64 i = 2; i < 50; ++i)
    {
        records.push_back(std::make_shared<cluster::SchemaRecord>(i));
    }

    auto range_buckets = DB::StreamCallbackData::categorizeRecordsAccordingToSequenceRanges(records, sequence_ranges, 47);
    EXPECT_EQ(range_buckets.size(), 5);
    EXPECT_EQ(range_buckets[0].first.size(), 2);
    EXPECT_EQ(range_buckets[0].first[0]->getSN(), 2);
    EXPECT_EQ(range_buckets[0].first[1]->getSN(), 3);
    EXPECT_EQ(range_buckets[0].second.size(), 1);
    EXPECT_EQ(range_buckets[0].second[0], (DB::SequenceRange{Int64(2), Int64(3)}));

    EXPECT_EQ(range_buckets[1].first.size(), 4);
    EXPECT_EQ(range_buckets[1].first[0]->getSN(), 10);
    EXPECT_EQ(range_buckets[1].first[1]->getSN(), 11);
    EXPECT_EQ(range_buckets[1].first[2]->getSN(), 12);
    EXPECT_EQ(range_buckets[1].first[3]->getSN(), 13);
    EXPECT_EQ(range_buckets[1].second.size(), 1);
    EXPECT_EQ(range_buckets[1].second[0], (DB::SequenceRange{Int64(10), Int64(13)}));

    EXPECT_EQ(range_buckets[2].first.size(), 5);
    EXPECT_EQ(range_buckets[2].first[0]->getSN(), 19);
    EXPECT_EQ(range_buckets[2].first[1]->getSN(), 20);
    EXPECT_EQ(range_buckets[2].first[2]->getSN(), 21);
    EXPECT_EQ(range_buckets[2].first[3]->getSN(), 22);
    EXPECT_EQ(range_buckets[2].first[4]->getSN(), 23);
    EXPECT_EQ(range_buckets[2].second.size(), 1);
    EXPECT_EQ(range_buckets[2].second[0], (DB::SequenceRange{19, 23, 2, 4}));

    EXPECT_EQ(range_buckets[3].first.size(), 6);
    EXPECT_EQ(range_buckets[3].first[0]->getSN(), 38);
    EXPECT_EQ(range_buckets[3].first[1]->getSN(), 39);
    EXPECT_EQ(range_buckets[3].first[2]->getSN(), 40);
    EXPECT_EQ(range_buckets[3].first[3]->getSN(), 41);
    EXPECT_EQ(range_buckets[3].first[4]->getSN(), 42);
    EXPECT_EQ(range_buckets[3].first[5]->getSN(), 43);
    EXPECT_EQ(range_buckets[3].second.size(), 2);
    EXPECT_EQ(range_buckets[3].second[0], (DB::SequenceRange{38, 43, 1, 5}));
    EXPECT_EQ(range_buckets[3].second[1], (DB::SequenceRange{38, 43, 3, 5}));

    EXPECT_EQ(range_buckets[4].first.size(), 2);
    EXPECT_EQ(range_buckets[4].first[0]->getSN(), 48);
    EXPECT_EQ(range_buckets[4].first[1]->getSN(), 49);
    EXPECT_TRUE(range_buckets[4].second.empty());
}

namespace
{
using namespace DB;

cluster::SchemaRecordPtr makeRecord(Int64 sn, size_t payload_bytes = 8)
{
    auto record = std::make_shared<cluster::SchemaRecord>(sn);
    auto column = ColumnString::create();
    column->insert(std::string(payload_bytes, 'x'));
    Block block;
    block.insert({std::move(column), std::make_shared<DataTypeString>(), "payload"});
    /// setBlock() stamps the current append time, which asserts unless it was set first.
    record->setAppendTime(1);
    record->setBlock(std::move(block));
    return record;
}

cluster::SchemaRecordPtrs makeBatch(Int64 from, Int64 to, size_t payload_bytes = 8)
{
    cluster::SchemaRecordPtrs batch;
    batch.reserve(to - from + 1);

    for (Int64 sn = from; sn <= to; ++sn)
        batch.push_back(makeRecord(sn, payload_bytes));

    return batch;
}

/// Drives the real StreamCallbackData through its test seam and captures every doCommit exactly as
/// StreamShardStore::commit would receive it.
struct RecoveryFixture
{
    std::vector<RecordsSequenceRangesPair> commits;
    StreamCallbackData callback;

    RecoveryFixture(
        const SequenceRanges & missing,
        Int64 max_committed_sn,
        size_t bytes_threshold = std::numeric_limits<size_t>::max(),
        size_t rows_threshold = std::numeric_limits<size_t>::max(),
        Int64 ms_threshold = std::numeric_limits<Int64>::max())
        : callback(
            missing,
            bytes_threshold,
            rows_threshold,
            ms_threshold,
            [max_committed_sn] { return max_committed_sn; },
            [this](cluster::SchemaRecordPtrs records, SequenceRanges ranges)
            { commits.emplace_back(std::move(records), std::move(ranges)); },
            getLogger("StreamingRecoveryTest"))
    {
    }
};

std::map<Int64, int> commitCount(const std::vector<RecordsSequenceRangesPair> & commits)
{
    std::map<Int64, int> count;
    for (const auto & bucket : commits)
        for (const auto & record : bucket.first)
            ++count[record->getSN()];

    return count;
}

/// Canonical form of a bucket: sorted sns of its records paired with its sequence ranges, so buckets
/// can be compared irrespective of the order flushes emit them in.
std::pair<std::vector<Int64>, SequenceRanges> canonical(const RecordsSequenceRangesPair & bucket)
{
    std::vector<Int64> sns;
    sns.reserve(bucket.first.size());

    for (const auto & r : bucket.first)
        sns.push_back(r->getSN());

    std::sort(sns.begin(), sns.end());
    return {std::move(sns), bucket.second};
}
}

/// With flush thresholds larger than the backlog, streaming degenerates to a single flush at the
/// max_committed_sn crossing, so it must reproduce the original single-shot categorization exactly -
/// same buckets, same records, same per-bucket sequence ranges. This pins the equivalence claim.
TEST(StreamingRecovery, EquivalentToSingleShotWhenFlushedOnce)
{
    SequenceRanges missing;
    missing.emplace_back(Int64(2), Int64(3));
    missing.emplace_back(Int64(10), Int64(13));
    missing.emplace_back(19, 23, 2, 4);
    missing.emplace_back(38, 43, 1, 5);
    missing.emplace_back(38, 43, 3, 5);

    cluster::SchemaRecordPtrs all_records;
    for (Int64 i = 2; i < 50; ++i)
        all_records.push_back(makeRecord(i));

    auto single_shot = StreamCallbackData::categorizeRecordsAccordingToSequenceRanges(all_records, missing, 47);

    RecoveryFixture fx(missing, /*max_committed_sn=*/47);

    /// Feed in small batches to prove the admission filter and buffering are order-robust.
    for (size_t i = 0; i < all_records.size(); i += 3)
    {
        cluster::SchemaRecordPtrs batch(all_records.begin() + i, all_records.begin() + std::min(i + 3, all_records.size()));
        fx.callback.commit(std::move(batch));
    }

    ASSERT_EQ(fx.commits.size(), single_shot.size());

    for (size_t i = 0; i < single_shot.size(); ++i)
        EXPECT_EQ(canonical(fx.commits[i]), canonical(single_shot[i])) << "bucket=" << i;

    EXPECT_TRUE(fx.callback.recoveryFinished());
    EXPECT_EQ(fx.callback.recoveryBufferRows(), 0);
}

/// A multi-part missing range (several SequenceRanges sharing a start_sn) that a threshold-driven
/// flush would cut through must instead be held until fully seen and then committed as one coherent
/// bucket carrying all its part ranges - never split across two commits.
TEST(StreamingRecovery, MultiPartRangeNotSplitAcrossFlushBoundary)
{
    SequenceRanges missing;
    /// One logical write [100,105] split into 4 parts, of which part 1 and part 3 are missing.
    missing.emplace_back(100, 105, 1, 4);
    missing.emplace_back(100, 105, 3, 4);

    RecoveryFixture fx(missing, /*max_committed_sn=*/1000, std::numeric_limits<size_t>::max(), /*rows_threshold=*/2);

    /// Feed the first half of the range; the tiny row threshold would love to flush here.
    fx.callback.commit(makeBatch(100, 102));

    /// The whole multi-part range is still incomplete, so nothing is committed and the records stay
    /// buffered as one indivisible unit.
    EXPECT_EQ(StreamCallbackData::recoveryFlushBoundarySN(missing, 102), 100);
    EXPECT_TRUE(fx.commits.empty());
    EXPECT_EQ(fx.callback.recoveryBufferRows(), 3);

    /// Feed the rest plus a record beyond the range so it is provably complete.
    fx.callback.commit(makeBatch(103, 106));

    /// Exactly one bucket, holding all six records 100..105 and both part ranges - not split.
    ASSERT_EQ(fx.commits.size(), 1);
    ASSERT_EQ(fx.commits[0].first.size(), 6);

    for (size_t i = 0; i < 6; ++i)
        EXPECT_EQ(fx.commits[0].first[i]->getSN(), 100 + static_cast<Int64>(i));

    ASSERT_EQ(fx.commits[0].second.size(), 2);
    EXPECT_EQ(fx.commits[0].second[0], (SequenceRange{100, 105, 1, 4}));
    EXPECT_EQ(fx.commits[0].second[1], (SequenceRange{100, 105, 3, 4}));
}

/// Incident shape: committed_sn pinned by a couple of tiny holes while parts already cover a far-away
/// max_committed_sn, so recovery re-consumes the whole interval. Streaming admission drops the
/// already-durable records as they arrive, so peak buffer stays O(hole size), not O(backlog).
TEST(StreamingRecovery, DeepBacklogBufferStaysBounded)
{
    SequenceRanges missing;
    missing.emplace_back(Int64(101), Int64(102));
    missing.emplace_back(Int64(500), Int64(500));

    constexpr Int64 start_sn = 100;
    constexpr Int64 max_committed_sn = 50000;
    constexpr Int64 batch_rows = 1000;

    RecoveryFixture fx(missing, max_committed_sn, std::numeric_limits<size_t>::max(), /*rows_threshold=*/8192);

    size_t naive_rows = 0; /// what buffering every re-consumed record would hold
    size_t peak_buffer_rows = 0;

    for (Int64 from = start_sn; from <= max_committed_sn + 1; from += batch_rows)
    {
        auto batch = makeBatch(from, std::min<Int64>(from + batch_rows - 1, max_committed_sn + 1), /*payload_bytes=*/1024);
        naive_rows += batch.size();
        fx.callback.commit(std::move(batch));
        peak_buffer_rows = std::max(peak_buffer_rows, fx.callback.recoveryBufferRows());
    }

    /// Naive full-buffering holds the whole interval; streaming holds only the 3 hole records plus
    /// the single record beyond max_committed_sn (all flushed by the crossing).
    EXPECT_GE(naive_rows, size_t(49000));
    EXPECT_LE(peak_buffer_rows, size_t(16));
    EXPECT_TRUE(fx.callback.recoveryFinished());

    /// The three hole records and the one new record are all committed exactly once.
    auto count = commitCount(fx.commits);
    EXPECT_EQ(count.size(), 4);

    for (Int64 sn : {101, 102, 500, 50001})
        EXPECT_EQ(count[sn], 1) << "sn=" << sn;
}

/// A single large pure gap (parts < 0, so every part is persisted and it imposes no flush boundary)
/// must flush incrementally instead of buffering the whole gap. Assert the buffer never exceeds
/// ~one flush threshold while the whole gap is committed exactly once.
TEST(StreamingRecovery, SingleLargePureGapFlushesIncrementally)
{
    constexpr Int64 gap_start = 100;
    constexpr Int64 gap_end = 100000;
    constexpr Int64 max_committed_sn = gap_end; /// gap end is at the committed frontier
    constexpr Int64 batch_rows = 500;
    constexpr size_t flush_threshold_rows = 2000;

    SequenceRanges missing;
    missing.emplace_back(gap_start, gap_end); /// pure gap: part_index=-1, parts=-1

    RecoveryFixture fx(missing, max_committed_sn, std::numeric_limits<size_t>::max(), flush_threshold_rows);

    size_t peak_buffer_rows = 0;

    /// Feed the whole gap plus one record beyond max_committed_sn so recovery provably completes.
    for (Int64 from = gap_start; from <= max_committed_sn + 1; from += batch_rows)
    {
        fx.callback.commit(makeBatch(from, std::min<Int64>(from + batch_rows - 1, max_committed_sn + 1)));
        peak_buffer_rows = std::max(peak_buffer_rows, fx.callback.recoveryBufferRows());
    }

    /// Memory bounded by the flush threshold plus at most one in-flight batch, not the 100k backlog.
    EXPECT_LE(peak_buffer_rows, flush_threshold_rows + batch_rows);
    EXPECT_TRUE(fx.callback.recoveryFinished());

    /// Every gap record gap_start..gap_end plus the single new record is committed exactly once.
    auto count = commitCount(fx.commits);

    for (Int64 sn = gap_start; sn <= gap_end; ++sn)
        ASSERT_EQ(count[sn], 1) << "gap sn=" << sn;

    EXPECT_EQ(count[max_committed_sn + 1], 1);
    EXPECT_EQ(count.size(), size_t(gap_end - gap_start + 1) + 1);
}

/// The bytes axis of the flush threshold must trigger incremental commits on its own, well before
/// the crossing, and drain the flushable buffer.
TEST(StreamingRecovery, FlushOnBytesThreshold)
{
    SequenceRanges missing;
    missing.emplace_back(Int64(100), Int64(10000));

    RecoveryFixture fx(missing, /*max_committed_sn=*/10000, /*bytes_threshold=*/4096);

    fx.callback.commit(makeBatch(100, 199, /*payload_bytes=*/1024));

    EXPECT_FALSE(fx.commits.empty());
    EXPECT_EQ(fx.callback.recoveryBufferBytes(), 0);
    EXPECT_EQ(commitCount(fx.commits).size(), 100);
}

/// The time axis of the flush threshold must trigger incremental commits on its own.
TEST(StreamingRecovery, FlushOnTimeThreshold)
{
    SequenceRanges missing;
    missing.emplace_back(Int64(100), Int64(10000));

    RecoveryFixture fx(
        missing,
        /*max_committed_sn=*/10000,
        std::numeric_limits<size_t>::max(),
        std::numeric_limits<size_t>::max(),
        /*ms_threshold=*/0);

    fx.callback.commit(makeBatch(100, 149));

    EXPECT_FALSE(fx.commits.empty());
    EXPECT_EQ(commitCount(fx.commits).size(), 50);
}

/// The canonical crash artifact: the LAST write was torn, so the hole's end_sn equals
/// max_committed_sn exactly and the log has no record beyond it. In-order delivery means seeing
/// sn == end_sn proves the range fully seen, so recovery must commit it and finish right there -
/// waiting for a record strictly beyond max_committed_sn would leave the hole unrepaired and
/// committed_sn pinned forever on a write-idle stream.
TEST(StreamingRecovery, TailMultiPartHoleAtMaxCommittedSN)
{
    SequenceRanges missing;
    missing.emplace_back(100, 105, 1, 4);
    missing.emplace_back(100, 105, 3, 4);

    /// Thresholds stay unlimited so only the crossing can flush.
    RecoveryFixture fx(missing, /*max_committed_sn=*/105);

    /// The whole remaining log; nothing newer exists.
    fx.callback.commit(makeBatch(100, 105));

    ASSERT_EQ(fx.commits.size(), 1);
    ASSERT_EQ(fx.commits[0].first.size(), 6);
    ASSERT_EQ(fx.commits[0].second.size(), 2);
    EXPECT_EQ(fx.callback.recoveryBufferRows(), 0);
    EXPECT_TRUE(fx.callback.recoveryFinished());
}

/// Same tail shape with a pure gap: the gap's records must flush and the gap retire at the
/// crossing, without a record beyond max_committed_sn.
TEST(StreamingRecovery, TailPureGapHoleAtMaxCommittedSN)
{
    SequenceRanges missing;
    missing.emplace_back(Int64(103), Int64(105));

    RecoveryFixture fx(missing, /*max_committed_sn=*/105);

    fx.callback.commit(makeBatch(100, 105));

    ASSERT_EQ(fx.commits.size(), 1);
    ASSERT_EQ(fx.commits[0].first.size(), 3);
    EXPECT_EQ(fx.commits[0].first[0]->getSN(), 103);
    EXPECT_TRUE(fx.callback.recoveryFinished());
}

/// Mid-recovery crash idempotency: incremental commit fills some holes, then the process restarts,
/// re-runs committed_sn reconciliation (recomputing the smaller hole set), and re-consumes from the
/// low watermark. Already-filled records must dedup (dropped by admission) and unfilled ones commit
/// once - no loss, no duplicate.
TEST(StreamingRecovery, MidRecoveryCrashIsIdempotent)
{
    SequenceRanges original_missing;
    original_missing.emplace_back(Int64(100), Int64(105)); /// pure gap A
    original_missing.emplace_back(Int64(200), Int64(205)); /// pure gap B

    constexpr Int64 max_committed_sn = 300;

    /// Run 1: consume up to just past gap A, then "crash" (stop feeding). Gap A flushes.
    RecoveryFixture run1(original_missing, max_committed_sn, std::numeric_limits<size_t>::max(), /*rows_threshold=*/1);
    run1.callback.commit(makeBatch(100, 110));

    auto count = commitCount(run1.commits);

    for (Int64 sn = 100; sn <= 105; ++sn)
        EXPECT_EQ(count[sn], 1) << "run1 sn=" << sn;

    EXPECT_FALSE(run1.callback.recoveryFinished()); /// gap B still pending

    /// Restart: reconciliation recomputes the hole set. Gap A is now durable, so only gap B remains.
    SequenceRanges recomputed_missing;
    recomputed_missing.emplace_back(Int64(200), Int64(205));

    RecoveryFixture run2(recomputed_missing, max_committed_sn, std::numeric_limits<size_t>::max(), /*rows_threshold=*/1);

    /// Re-consume from the low watermark: records 100..105 are re-delivered but already durable.
    run2.callback.commit(makeBatch(100, 301));

    for (const auto & [sn, n] : commitCount(run2.commits))
        count[sn] += n;

    /// Every sequence number in either gap is committed exactly once across both runs.
    for (Int64 sn = 100; sn <= 105; ++sn)
        EXPECT_EQ(count[sn], 1) << "gap A sn=" << sn;

    for (Int64 sn = 200; sn <= 205; ++sn)
        EXPECT_EQ(count[sn], 1) << "gap B sn=" << sn;

    /// Already-committed records between/around the gaps are dropped by admission on both runs and
    /// never re-committed.
    EXPECT_EQ(count.count(106), 0);
    EXPECT_EQ(count.count(199), 0);
    EXPECT_EQ(count.count(206), 0);

    /// The one genuinely new record beyond max_committed_sn is committed once.
    EXPECT_EQ(count[301], 1);
    EXPECT_TRUE(run2.callback.recoveryFinished());
}
