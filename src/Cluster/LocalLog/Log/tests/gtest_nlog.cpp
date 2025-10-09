#include <Cluster/Base/ByteVector.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/LocalLog/Log/Log.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <Common/BackgroundSchedulePool.h>
#include <Common/ProtonCommon.h>
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
extern const Metric BackgroundSchedulePoolNativeLogTask;
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace
{

class LogTestFixtureBase : public ::testing::Test
{
public:
    LogTestFixtureBase() = default;

    ~LogTestFixtureBase() override = default;

    void SetUp() override
    {
        cleanupLogDir();
        doSetUp();
    }

    void TearDown() override
    {
        doTearDown();
        cleanupLogDir();
    }

    void Reboot()
    {
        doTearDown();
        doSetUp();
    }

protected:
    cluster::nlog::LogPtr createLog()
    {
        cluster::StreamID stream_id;
        cluster::StreamShard stream_shard("test", "test", stream_id, 0);
        bool had_clean_shutdown = true;

        return cluster::nlog::Log::create(stream_shard, log_dir, logConfig(), had_clean_shutdown, *scheduler, *thread_pool);
    }

    void doSetUp()
    {
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 8);
        scheduler = std::make_unique<DB::NLOG::BackgroundSchedulePool>(8, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "SchedTest");
        log = createLog();
    }

    void doTearDown()
    {
        thread_pool.reset();
        scheduler.reset();

        log.reset();
    }

    void cleanupLogDir() const
    {
        std::error_code ec;
        fs::remove_all(log_dir, ec);
    }

protected:
    virtual cluster::nlog::LogConfigPtr logConfig() = 0;

    fs::path log_dir = "/tmp/nativelog/test/";

    static constexpr size_t data_size = 1024;

    cluster::nlog::LogPtr log;
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;
};

class LogTestFixture : public LogTestFixtureBase
{
private:
    cluster::nlog::LogConfigPtr logConfig() override { return std::make_shared<cluster::nlog::LogConfig>(); }
};

class LogTestFixtureInvertedIndex : public LogTestFixtureBase
{
private:
    cluster::nlog::LogConfigPtr logConfig() override
    {
        auto config = std::make_shared<cluster::nlog::LogConfig>();
        /// Every log entry we may have one inverted index entry
        config->index_interval_bytes = data_size;
        return config;
    }
};

class LogTestFixtureInvertedIndexTriple : public LogTestFixtureBase
{
private:
    cluster::nlog::LogConfigPtr logConfig() override
    {
        auto config = std::make_shared<cluster::nlog::LogConfig>();
        /// Every log entry we may have one inverted index entry
        config->index_interval_bytes = data_size * 3;
        return config;
    }
};

class LogTestFixtureInvertedIndexMultiSegments : public LogTestFixtureBase
{
private:
    cluster::nlog::LogConfigPtr logConfig() override
    {
        auto config = std::make_shared<cluster::nlog::LogConfig>();
        config->index_interval_bytes = data_size * 3;
        config->segment_size = data_size * 4;
        return config;
    }
};

class LogTestFixtureInvertedIndexTTL : public LogTestFixtureBase
{
private:
    cluster::nlog::LogConfigPtr logConfig() override
    {
        auto config = std::make_shared<cluster::nlog::LogConfig>();
        config->index_interval_bytes = data_size * 3;
        config->segment_size = data_size * 4;
        config->retention_size = 3 * data_size;
        config->min_size_to_keep = 0;
        return config;
    }
};

DB::Block createBlock(size_t value, size_t data_size, int64_t event_time)
{
    DB::Block block;

    auto uint64_type = std::make_shared<DB::DataTypeUInt64>();
    auto string_type = std::make_shared<DB::DataTypeString>();
    auto datetime64_type = std::make_shared<DB::DataTypeDateTime64>(3);

    auto id_col = uint64_type->createColumn();
    auto * id_col_inner = typeid_cast<DB::ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(value);
    block.insert(DB::ColumnWithTypeAndName{std::move(id_col), uint64_type, "id"});

    auto data_col = string_type->createColumn();
    std::string data(data_size, '+');
    data_col->insertData(data.data(), data.size());

    block.insert(DB::ColumnWithTypeAndName{std::move(data_col), string_type, "data"});

    auto time_col = datetime64_type->createColumn();
    auto * time_col_inner = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(time_col.get());
    time_col_inner->insertValue(event_time);
    block.insert(DB::ColumnWithTypeAndName{std::move(time_col), datetime64_type, DB::ProtonConsts::RESERVED_EVENT_TIME});

    return block;
}

cluster::EntryPtr createEntry(const DB::Block & block, bool compress)
{
    auto approx_size = static_cast<size_t>(block.bytes() * 1.15);
    cluster::ByteVector buffer(approx_size);
    {
        DB::WriteBufferFromVector wb(buffer);

        if (compress)
        {
            auto compression_codec = DB::CompressionMethodByte::ZSTD;
            DB::CompressedWriteBuffer compressed_wb(wb, DB::CompressionCodecFactory::instance().get(std::to_underlying(compression_codec)));

            DB::NativeWriter writer(compressed_wb, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);
        }
        else
        {
            DB::NativeWriter writer(wb, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);
        }
    }
    /// std::cout << fmt::format("block_approx_size={}, actual={} capacity={}", approx_size, buffer.size(), buffer.capacity()) << "\n";

    auto entry = std::make_shared<cluster::Entry>(std::move(buffer));
    entry->version = 2;
    entry->max_event_timestamp = 111;
    entry->append_timestamp = 222;
    entry->sn = cluster::Constants::LogStartSN;
    entry->type = cluster::EntryType::Normal;
    return entry;
}

void checkBlock(const DB::Block & expect, const DB::Block & actual)
{
    SipHash hash_expected;
    expect.updateHash(hash_expected);

    SipHash hash_got;
    actual.updateHash(hash_got);

    EXPECT_EQ(hash_expected.get64(), hash_got.get64());

    /// Compare columns
    for (const auto & col_expected : expect)
    {
        const auto & col_got = actual.getByName(col_expected.name);

        EXPECT_EQ(col_expected.name, col_got.name);
        EXPECT_EQ(col_expected.type->getTypeId(), col_got.type->getTypeId());
        EXPECT_EQ(col_expected.column->size(), col_got.column->size());

        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 0);
    }
}

DB::Block checkEntry(cluster::EntryPtr expect_entry, cluster::EntryPtr actual, bool compressed)
{
    EXPECT_EQ(expect_entry->serializedSize(), actual->serializedSize());
    EXPECT_EQ(expect_entry->getCRC(), actual->getCRC());
    EXPECT_EQ(expect_entry->version, actual->version);
    EXPECT_EQ(expect_entry->max_event_timestamp, actual->max_event_timestamp);
    EXPECT_EQ(expect_entry->append_timestamp, actual->append_timestamp);
    EXPECT_EQ(expect_entry->sn, actual->sn);
    EXPECT_EQ(expect_entry->type, actual->type);
    EXPECT_EQ(expect_entry->data.size(), actual->data.size());
    EXPECT_FALSE(std::memcmp(expect_entry->data.data(), actual->data.data(), expect_entry->data.size()));

    /// deserialize block
    DB::ReadBufferFromString rb(actual->data.stringView());
    if (compressed)
    {
        DB::CompressedReadBuffer compressed_rb(rb);
        DB::NativeReader reader(compressed_rb, DBMS_TCP_PROTOCOL_VERSION);
        return reader.read();
    }
    else
    {
        DB::NativeReader reader(rb, DBMS_TCP_PROTOCOL_VERSION);
        return reader.read();
    }
}

namespace
{
void testAppendAndFetchBackFromCache(cluster::nlog::LogPtr log, size_t data_size, bool compress)
{
    auto expected_block = createBlock(0, data_size, 1612286044);
    auto expected_entry = createEntry(expected_block, compress);

    auto err = log->append({expected_entry});

    ASSERT_TRUE(!err.hasError());

    auto expect_entry = [&](const cluster::nlog::FetchResult & fetch_result) {
        EXPECT_EQ(fetch_result.fetch_hint.sn, cluster::Constants::LogStartSN + 1);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.file_position, expected_entry->serializedSize());

        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.file_position, 0);

        ASSERT_EQ(fetch_result.entries.size(), 1);
        auto block = checkEntry(fetch_result.entries.back(), expected_entry, compress);
        checkBlock(block, expected_block);
    };

    /// Fetch LogEnd, shall get the newly appended log entry
    {
        auto fetch_result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);
        expect_entry(fetch_result.result);
    }

    /// Fetch commit, since we didn't commit the log entry
    {
        auto result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::Committed);
        const auto & fetch_result = result.result;
        EXPECT_EQ(fetch_result.fetch_hint.sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, 1);
        EXPECT_FALSE(fetch_result.fetch_hint.hasFilePosition());
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, 0);
        EXPECT_FALSE(fetch_result.fetched_log_sn_metadata.segment_base_sn.has_value());
        EXPECT_FALSE(fetch_result.fetched_log_sn_metadata.file_position.has_value());
        EXPECT_TRUE(fetch_result.entries.empty());
    }

    /// Progress the commit sn and fetch again, shall get the log entry
    {
        auto prev_committed_sn = log->advanceCommittedSequence(cluster::Constants::LogStartEpoch, cluster::Constants::LogStartSN);
        EXPECT_EQ(prev_committed_sn, cluster::Constants::LogStartSN - 1);

        auto fetch_result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::Committed);
        expect_entry(fetch_result.result);
    }
}
}

TEST_F(LogTestFixture, AppendAndFetchBackFromCacheCompressed)
{
    testAppendAndFetchBackFromCache(log, data_size, /*compress=*/true);
}

TEST_F(LogTestFixture, AppendAndFetchBackFromCacheUncompressed)
{
    testAppendAndFetchBackFromCache(log, data_size, /*compress=*/false);
}

namespace
{
void testAppendAndFetchBackFromFileSystem(cluster::nlog::LogPtr & log, size_t data_size, std::function<void()> reboot, bool compress)
{
    auto expected_block = createBlock(0, data_size, 1612286044);
    auto expected_entry = createEntry(expected_block, compress);

    auto err = log->append({expected_entry});

    EXPECT_TRUE(!err.hasError());

    /// Reboot to clear cache
    reboot();

    auto expect_entry = [&](const cluster::nlog::FetchResult & fetch_result) {
        EXPECT_EQ(fetch_result.fetch_hint.sn, cluster::Constants::LogStartSN + 1);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.file_position, expected_entry->serializedSize());

        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.file_position, 0);

        ASSERT_EQ(fetch_result.entries.size(), 1);
        auto block = checkEntry(fetch_result.entries.back(), expected_entry, compress);
        checkBlock(block, expected_block);
    };

    /// Fetch LogEnd, shall get the newly appended log entry
    {
        auto fetch_result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);
        expect_entry(fetch_result.result);
    }

    /// Fetch commit, after reboot, we set the commit ns to LogStartSN - 1
    {
        log->advanceCommittedSequence(cluster::Constants::LogStartEpoch, cluster::Constants::LogStartSN);
        auto fetch_result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::Committed);
        expect_entry(fetch_result.result);
    }
}
}

TEST_F(LogTestFixture, AppendAndFetchBackFromFileSystemCompressed)
{
    testAppendAndFetchBackFromFileSystem(log, data_size, [&]() { Reboot(); }, /*compress=*/true);
}

TEST_F(LogTestFixture, AppendAndFetchBackFromFileSystemUncompressed)
{
    testAppendAndFetchBackFromFileSystem(log, data_size, [&]() { Reboot(); }, /*compress=*/false);
}

TEST_F(LogTestFixture, TrimAll)
{
    /// Trim all
    {
        auto block = createBlock(0, data_size, 1612286044);
        auto entry = createEntry(block, false);

        auto err = log->append({entry});

        ASSERT_TRUE(!err.hasError());

        log->advanceCommittedSequence(cluster::Constants::LogStartEpoch, cluster::Constants::LogStartSN);

        auto errcode = log->trim(cluster::Constants::LogStartSN);
        ASSERT_EQ(errcode, DB::ErrorCodes::OK);

        auto result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);

        const auto & fetch_result = result.result;
        EXPECT_EQ(fetch_result.fetch_hint.sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.file_position, 0);

        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, 0);
        EXPECT_FALSE(fetch_result.fetched_log_sn_metadata.segment_base_sn.has_value());
        EXPECT_FALSE(fetch_result.fetched_log_sn_metadata.file_position.has_value());

        EXPECT_TRUE(fetch_result.entries.empty());

        auto next_log_sn_meta = log->nextLogSequenceMetadata();
        EXPECT_EQ(next_log_sn_meta.entry_sn, cluster::Constants::LogStartSN);
        ASSERT_TRUE(next_log_sn_meta.file_position);
        EXPECT_EQ(next_log_sn_meta.file_position.value(), 0);

        auto active = log->activeSegment();
        ASSERT_TRUE(active);
        EXPECT_EQ(active->size(), 0);
        auto res = active->fsize();
        ASSERT_FALSE(res.hasError());
        EXPECT_EQ(res.result, active->size());
    }
}

TEST_F(LogTestFixture, TrimTwo)
{
    auto expect_entry = [&](const cluster::nlog::FetchResult & fetch_result,
                            const cluster::EntryPtr & expected_entry,
                            const DB::Block & expected_block,
                            int64_t expected_next_sn,
                            int64_t expected_sn,
                            size_t expected_size,
                            size_t index) {
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, expected_sn);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.file_position, 0);

        EXPECT_EQ(fetch_result.fetch_hint.sn, expected_next_sn);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.file_position, expected_entry->serializedSize());

        ASSERT_EQ(fetch_result.entries.size(), expected_size);
        auto block = checkEntry(fetch_result.entries[index], expected_entry, false);
    };

    /// Trim two
    {
        cluster::EntryPtrs append_entries;
        std::vector<DB::Block> append_blocks;

        append_entries.reserve(3);
        append_blocks.reserve(3);

        for (size_t i = 0; i < 3; ++i)
        {
            auto block = createBlock(i, data_size, 1612286044 + i);

            auto entry = createEntry(block, false);
            entry->sn = cluster::Constants::LogStartSN + i;

            auto err = log->append({entry});
            ASSERT_TRUE(!err.hasError());

            append_entries.push_back(std::move(entry));
            append_blocks.push_back(std::move(block));
        }

        /// [LogStartSN + 1, LogStartSN + 2]
        log->advanceCommittedSequence(cluster::Constants::LogStartEpoch, cluster::Constants::LogStartSN + 1);
        auto errcode = log->trim(cluster::Constants::LogStartSN + 1);
        ASSERT_EQ(errcode, DB::ErrorCodes::OK);

        auto fetch_result
            = log->fetch(/*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);

        expect_entry(
            fetch_result.result,
            append_entries[0],
            append_blocks[0],
            /*expected_next_sn=*/cluster::Constants::LogStartSN + 1,
            /*expected_sn=*/cluster::Constants::LogStartSN,
            /*expected_size=*/1,
            /*index=*/0);

        auto active = log->activeSegment();
        ASSERT_TRUE(active);
        EXPECT_EQ(active->size(), fetch_result.result.fetch_hint.file_position);

        auto res = active->fsize();
        ASSERT_FALSE(res.hasError());
        EXPECT_EQ(res.result, active->size());
    }
}

TEST_F(LogTestFixture, TrimTrailing)
{
    auto expect_entry = [&](const cluster::nlog::FetchResult & fetch_result,
                            const cluster::EntryPtr & expected_entry,
                            const DB::Block & expected_block,
                            int64_t expected_next_sn,
                            int64_t expected_sn,
                            size_t expected_size,
                            size_t expected_next_file_position,
                            size_t index) {
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.entry_sn, expected_sn);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetched_log_sn_metadata.file_position, 0);

        EXPECT_EQ(fetch_result.fetch_hint.sn, expected_next_sn);
        EXPECT_EQ(fetch_result.fetch_hint.segment_base_sn, cluster::Constants::LogStartSN);
        EXPECT_EQ(fetch_result.fetch_hint.file_position, expected_next_file_position);

        ASSERT_EQ(fetch_result.entries.size(), expected_size);
        auto block = checkEntry(fetch_result.entries[index], expected_entry, false);
    };

    /// Trim trailing one
    {
        cluster::EntryPtrs append_entries;
        std::vector<DB::Block> append_blocks;

        append_entries.reserve(3);
        append_blocks.reserve(3);

        for (size_t i = 0; i < 3; ++i)
        {
            auto block = createBlock(i, data_size, 1612286044 + i);
            auto entry = createEntry(block, false);
            entry->sn = cluster::Constants::LogStartSN + i;

            auto err = log->append({entry});
            ASSERT_TRUE(!err.hasError());

            append_blocks.push_back(std::move(block));
            append_entries.push_back(std::move(entry));
        }

        /// [LogStartSN + 2]
        log->advanceCommittedSequence(cluster::Constants::LogStartEpoch, cluster::Constants::LogStartSN + 2);
        auto errcode = log->trim(cluster::Constants::LogStartSN + 2);
        ASSERT_EQ(errcode, DB::ErrorCodes::OK);

        auto fetch_result = log->fetch(
            /*sn=*/cluster::Constants::LogStartSN, /*max_size=*/10000000, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);

        auto expected_next_file_position = append_entries[0]->serializedSize() + append_entries[1]->serializedSize();

        for (size_t i = 0; i < 2; ++i)
            expect_entry(
                fetch_result.result,
                append_entries[i],
                append_blocks[i],
                /*expected_next_sn=*/cluster::Constants::LogStartSN + 2,
                /*expected_sn=*/cluster::Constants::LogStartSN,
                /*expected_size=*/2,
                expected_next_file_position,
                /*index=*/i);

        auto active = log->activeSegment();
        ASSERT_TRUE(active);
        EXPECT_EQ(active->size(), fetch_result.result.fetch_hint.file_position);

        auto res = active->fsize();
        ASSERT_FALSE(res.hasError());
        EXPECT_EQ(res.result, active->size());
    }
}

TEST_F(LogTestFixtureInvertedIndex, ReverseLookupSequenceForTimestampEveryEntry)
{
    std::vector<int64_t> event_timestamps = {
        1612286044, /// 0, always index since it is the first entry
        1612286043, /// 1, no index entry since it is smaller timestamp
        1612286042, /// 2, no index entry since it is smaller timestamp
        1612286045, /// 3
        1612286041, /// 4, no index entry
        1612286040, /// 5, no index entry
        1612286046, /// 6
        1612286047, /// 7
        1612286048, /// 8
    };

    for (size_t i = 0; auto event_time : event_timestamps)
    {
        auto block = createBlock(i, data_size, event_time);
        auto entry = createEntry(block, false);
        entry->sn = cluster::Constants::LogStartSN + i;
        entry->max_event_timestamp = event_time;

        auto err = log->append({entry});

        ASSERT_TRUE(!err.hasError());

        ++i;
    }

    auto sn = log->sequenceForTimestamp(event_timestamps[0], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[1], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[2], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[3], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[4], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[5], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[6], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);

    sn = log->sequenceForTimestamp(event_timestamps[7], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 7);

    sn = log->sequenceForTimestamp(event_timestamps[8], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 8);
}

TEST_F(LogTestFixtureInvertedIndexTriple, ReverseLookupSequenceForTimestampTriple)
{
    std::vector<int64_t> event_timestamps = {
        1612286044, /// 0, always index since it is the first entry
        1612286045, /// 1, no index entry since every triple log entries, we have one index entry max
        1612286046, /// 2, no index entry
        1612286047, /// 3
        1612286048, /// 4, no index entry
        1612286049, /// 5, no index entry
        1612286050, /// 6
        1612286051, /// 7, no index entry
        1612286052, /// 8, no index entry
    };

    for (size_t i = 0; auto event_time : event_timestamps)
    {
        auto block = createBlock(i, data_size, event_time);
        auto entry = createEntry(block, false);
        entry->sn = cluster::Constants::LogStartSN + i;
        entry->max_event_timestamp = event_time;

        auto err = log->append({entry});

        ASSERT_TRUE(!err.hasError());

        ++i;
    }

    auto sn = log->sequenceForTimestamp(event_timestamps[0], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[1], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[2], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[3], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[4], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[5], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[6], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);

    sn = log->sequenceForTimestamp(event_timestamps[7], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);

    sn = log->sequenceForTimestamp(event_timestamps[8], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);
}

TEST_F(LogTestFixtureInvertedIndexMultiSegments, ReverseLookupSequenceForTimestampMultSegments)
{
    std::vector<int64_t> event_timestamps = {
        /// segment-0
        1612286044, /// 0, always index since it is the first entry
        1612286045, /// 1, no index entry since every triple log entries, we have one index entry max
        1612286046, /// 2, always index since we roll the segment
        /// segment-1
        1612286047, /// 3, always index since it is the first entry
        1612286048, /// 4, no index entry
        1612286049, /// 5, always index since we roll the segment
        /// segment-2
        1612286050, /// 6, always index since it is the first entry
        1612286051, /// 7, no index entry
        1612286052, /// 8, always index since we roll the segment
        /// segment-3
        1612286053, /// 9, always index since we roll the segment
    };

    for (size_t i = 0; auto event_time : event_timestamps)
    {
        auto block = createBlock(i, data_size, event_time);
        auto entry = createEntry(block, false);
        entry->sn = cluster::Constants::LogStartSN + i;
        entry->max_event_timestamp = event_time;

        auto err = log->append({entry});

        ASSERT_TRUE(!err.hasError());

        ++i;
    }

    auto sn = log->sequenceForTimestamp(event_timestamps[0], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[1], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN);

    sn = log->sequenceForTimestamp(event_timestamps[2], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 2);

    sn = log->sequenceForTimestamp(event_timestamps[3], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[4], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 3);

    sn = log->sequenceForTimestamp(event_timestamps[5], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 5);

    sn = log->sequenceForTimestamp(event_timestamps[6], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);

    sn = log->sequenceForTimestamp(event_timestamps[7], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 6);

    sn = log->sequenceForTimestamp(event_timestamps[8], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 8);

    sn = log->sequenceForTimestamp(event_timestamps[9], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);
}

TEST_F(LogTestFixtureInvertedIndexTTL, ReverseLookupSequenceForTimestampTTL)
{
    std::vector<int64_t> event_timestamps = {
        /// segment-0, GCed
        1612286044, /// 1, always index since it is the first entry
        1612286045, /// 2, no index entry since every triple log entries, we have one index entry max
        1612286046, /// 3, always index since we roll the segment
        /// segment-1, GCed
        1612286047, /// 4, always index since it is the first entry
        1612286048, /// 5, no index entry
        1612286049, /// 6, always index since we roll the segmena
        /// segment-2, GCed
        1612286050, /// 7, always index since it is the first entry
        1612286051, /// 8, no index entry
        1612286052, /// 9, always index since we roll the segment
        /// segment-3
        1612286053, /// 10, always index since we roll the segment
    };

    for (size_t i = 0; auto event_time : event_timestamps)
    {
        auto block = createBlock(i, data_size, event_time);
        auto entry = createEntry(block, false);
        entry->sn = cluster::Constants::LogStartSN + i;
        entry->max_event_timestamp = event_time;

        auto err = log->append({entry});

        ASSERT_TRUE(!err.hasError());

        ++i;
    }

    log->deleteOldSegments(log->nextLogSequence()); /// pass log end sn to force gc the last second segment for this test case purpose

    auto sn = log->sequenceForTimestamp(event_timestamps[0], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[1], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[2], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[3], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[4], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[5], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[6], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[7], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[8], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);

    sn = log->sequenceForTimestamp(event_timestamps[9], false);
    EXPECT_EQ(sn, cluster::Constants::LogStartSN + 9);
}

TEST_F(LogTestFixture, AppendBatchAndFetchBack)
{
    int64_t event_timestamp = 1612286044;
    int64_t append_timestamp = 1612286044;

    constexpr size_t n = 10'000;
    cluster::EntryPtrs entries;
    entries.reserve(n);

    for (size_t i = 0; i < n; ++i)
    {
        auto block = createBlock(i, data_size, event_timestamp + i);
        auto entry = createEntry(block, false);
        entry->max_event_timestamp = event_timestamp + i;
        entry->append_timestamp = append_timestamp + i;
        entry->sn = cluster::Constants::LogStartSN + i;
        entries.push_back(std::move(entry));
    }

    auto append_result = log->append(entries);
    ASSERT_TRUE(!append_result.hasError());

    /// Fetch LogEnd, shall get the newly appended log entries
    {
        auto fetch_result = log->fetch(
            /*sn=*/cluster::Constants::LogStartSN, /*max_size=*/1'000'000'000, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);

        ASSERT_EQ(fetch_result.result.entries.size(), entries.size());
        ASSERT_EQ(fetch_result.result.fetch_hint.sn, entries.back()->sn + 1);
        ASSERT_EQ(fetch_result.result.fetch_hint.file_position, append_result.result);

        for (size_t i = 0; const auto & entry : fetch_result.result.entries)
        {
            ASSERT_EQ(*entry, *entries[i]);
            ++i;
        }
    }
}

TEST_F(LogTestFixture, AppendBatchAndMultipleFetchBack)
{
    int64_t event_timestamp = 1612286044;
    int64_t append_timestamp = 1612286044;

    constexpr size_t n = 10'000;
    cluster::EntryPtrs entries;
    entries.reserve(n);

    for (size_t i = 0; i < n; ++i)
    {
        auto block = createBlock(i, data_size, event_timestamp + i);
        auto entry = createEntry(block, false);
        entry->max_event_timestamp = event_timestamp + i;
        entry->append_timestamp = append_timestamp + i;
        entry->sn = cluster::Constants::LogStartSN + i;
        entries.push_back(std::move(entry));
    }

    auto append_result = log->append(entries);
    ASSERT_TRUE(!append_result.hasError());

    /// Fetch LogEnd, shall get the newly appended log entries
    size_t batch_fetch_size = payloadSizeOfEntries(entries) / 10; /// ~ 10 batches
    size_t fetch_times = 0;
    size_t fetched_count = 0;
    Int64 next_start_sn = cluster::Constants::LogStartSN;
    while (true)
    {
        auto fetch_result = log->fetch(
            /*sn=*/next_start_sn, /*max_size=*/batch_fetch_size, /*max_wait_ms=*/0, {}, cluster::FetchIsolation::LogEnd);

        ++fetch_times;
        fetched_count += fetch_result.result.entries.size();
        next_start_sn = fetch_result.result.fetch_hint.sn;

        ASSERT_FALSE(fetch_result.hasError());

        if (fetch_result.result.entries.empty())
        {
            ASSERT_EQ(fetch_result.result.fetch_hint.file_position, append_result.result);
            break;
        }

        ASSERT_EQ(next_start_sn, entries.at(fetched_count - 1)->sn + 1);

        for (size_t i = fetched_count - fetch_result.result.entries.size(); const auto & entry : fetch_result.result.entries)
        {
            ASSERT_EQ(*entry, *entries[i]);
            ++i;
        }
    }

    ASSERT_TRUE(fetch_times >= 9);
    ASSERT_EQ(fetched_count, n);
}
}
