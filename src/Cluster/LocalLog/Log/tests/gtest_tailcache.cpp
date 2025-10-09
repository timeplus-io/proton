#include <gtest/gtest.h>

#include <Cluster/LocalLog/Log/LogEntryCache.h>


class LogEntryCacheZeroSizeTest : public ::testing::Test
{
protected:
    cluster::nlog::LogEntryCache cache{0};

    void SetUp() override { }

    void TearDown() override { }

    cluster::EntryPtr createEntry(int64_t sn) { return std::make_shared<cluster::Entry>(sn); }
};

class LogEntryCacheNormalSizeTest : public ::testing::Test
{
protected:
    cluster::nlog::LogEntryCache cache{5}; /// max_cached_entries_per_shard set to 5 for testing

    void SetUp() override
    {
        for (int64_t i = 1; i <= 5; ++i)
        {
            auto entry = createEntry(i);
            cluster::nlog::LogSequenceMetadata metadata(entry->sn, std::optional<int64_t>{0});
            cluster::FetchHint fetch_hint;
            fetch_hint.sn = i + 1; /// Set fetch_hint.sn to the next sequence number
            cache.append(entry, metadata, fetch_hint);
        }
    }

    void TearDown() override
    {
        /// Cleanup code, if needed
    }

    cluster::EntryPtr createEntry(int64_t sn) { return std::make_shared<cluster::Entry>(sn); }
};


/// Test appending a single entry to the cache
TEST_F(LogEntryCacheZeroSizeTest, AppendSingleEntry)
{
    auto entry = createEntry(1);
    cluster::nlog::LogSequenceMetadata metadata(entry->sn, std::optional<int64_t>{0});
    cluster::FetchHint fetch_hint;
    fetch_hint.sn = 2;
    cache.append(entry, metadata, fetch_hint);

    /// Expect that the cache remains empty
    auto last_sn = cache.lastLogSequence();
    ASSERT_FALSE(last_sn.has_value());
}

/// Test fetching from an empty cache
TEST_F(LogEntryCacheZeroSizeTest, FetchFromEmptyCache)
{
    auto [result, fallback] = cache.fetch(1, 2, 1, false);
    /// Expect fallback to file system because the cache should be empty
    ASSERT_TRUE(fallback);
    ASSERT_TRUE(result.entries.empty());
}

/// Test trimming an empty cache
TEST_F(LogEntryCacheZeroSizeTest, TrimEmptyCache)
{
    auto new_fetch_hint = cache.trim(1);
    /// Expect no change as there are no entries to trim
    ASSERT_FALSE(new_fetch_hint.has_value());
}

/// Test clearing an already empty cache
TEST_F(LogEntryCacheZeroSizeTest, ClearEmptyCache)
{
    cache.clear();
    auto last_sn = cache.lastLogSequence();
    /// Still expect no entries
    ASSERT_FALSE(last_sn.has_value());
}

/// Test lastLogSequence on an empty cache
TEST_F(LogEntryCacheZeroSizeTest, LastLogSequenceEmptyCache)
{
    auto last_sn = cache.lastLogSequence();
    /// Expect no last sequence number
    ASSERT_FALSE(last_sn.has_value());
}

/// Test appending a single entry
TEST_F(LogEntryCacheNormalSizeTest, AppendSingleEntry)
{
    auto last_sn = cache.lastLogSequence();

    {
        auto entry = std::make_shared<cluster::Entry>(last_sn.value() + 1);
        cluster::nlog::LogSequenceMetadata metadata;
        metadata.segment_base_sn = 0;
        cluster::FetchHint fetch_hint;
        fetch_hint.sn = last_sn.value() + 1;
        cache.append(entry, metadata, fetch_hint);
    }

    auto new_last_sn = cache.lastLogSequence();
    ASSERT_TRUE(new_last_sn.has_value());
    ASSERT_EQ(new_last_sn.value(), last_sn.value() + 1);
}

/// Test fetching a range of entries
TEST_F(LogEntryCacheNormalSizeTest, FetchRange)
{
    /// Fetch a range of entries
    auto [result, fallback] = cache.fetch(2, 4, std::numeric_limits<size_t>::max(), false);
    ASSERT_FALSE(fallback);
    ASSERT_EQ(result.entries.size(), 2);
    ASSERT_EQ(result.entries[0]->sn, 2);
    ASSERT_EQ(result.entries[1]->sn, 3);
}

/// Test fetching a range of entries with an entry size limit
TEST_F(LogEntryCacheNormalSizeTest, FetchRangeWithMaxSizeLimit)
{
    auto [result, fallback] = cache.fetch(2, 4, createEntry(2)->approximateSerializedSize(), false);
    ASSERT_FALSE(fallback);
    ASSERT_EQ(result.entries.size(), 1);
    ASSERT_EQ(result.entries[0]->sn, 2);
}

/// Test appending a batch of consecutive entries
TEST_F(LogEntryCacheNormalSizeTest, AppendBatchConsecutiveEntries)
{
    cache.clear();
    std::vector<cluster::EntryPtr> batch;
    std::vector<cluster::ByteVector> batch_data;
    for (int64_t i = 1; i <= 5; ++i)
    {
        batch.push_back(createEntry(i));
        batch_data.push_back(cluster::ByteVector(std::string(10, 'a')));
    }

    cache.append(std::span(batch.begin(), batch.end()), batch_data, 0, 0);

    auto last_sn = cache.lastLogSequence();
    ASSERT_TRUE(last_sn.has_value());
    ASSERT_EQ(last_sn.value(), 5);
}

/// Test fetching a range of entries with memory_log set to true
TEST_F(LogEntryCacheNormalSizeTest, FetchRangeMemoryLogTrue)
{
    /// Fetch a range of entries with memory_log set to true
    auto [result, fallback] = cache.fetch(2, 4, std::numeric_limits<size_t>::max(), true);
    ASSERT_FALSE(fallback);
    ASSERT_EQ(result.entries.size(), 2);
    ASSERT_EQ(result.entries[0]->sn, 2);
    ASSERT_EQ(result.entries[1]->sn, 3);
}

/// Test fetching a range of entries with memory_log set to true and with an entry size limit
TEST_F(LogEntryCacheNormalSizeTest, FetchRangeMemoryLogTrueWithMaxSizeLimit)
{
    auto [result, fallback] = cache.fetch(2, 4, createEntry(2)->approximateSerializedSize(), true);
    ASSERT_FALSE(fallback);
    ASSERT_EQ(result.entries.size(), 1);
    ASSERT_EQ(result.entries[0]->sn, 2);
}

/// Test trimming the cache with a target sequence number in the middle
TEST_F(LogEntryCacheNormalSizeTest, TrimCacheMiddleTarget)
{
    /// Trim the cache with a target sequence number in the middle
    auto new_fetch_hint = cache.trim(3);
    ASSERT_TRUE(new_fetch_hint.has_value());
    ASSERT_EQ(new_fetch_hint->sn, 3);

    /// Verify that entries up to the target sequence number are removed
    auto last_sn = cache.lastLogSequence();
    ASSERT_TRUE(last_sn.has_value());
    ASSERT_EQ(last_sn.value(), 2);
}

/// Trim a sn which small and not in the cache but less than the cache's smallest sn,
/// it shall trim all cached entries
TEST_F(LogEntryCacheNormalSizeTest, TrimAll)
{
    /// Trim the cache with a target sequence number in the middle
    auto new_fetch_hint = cache.trim(0);
    ASSERT_FALSE(new_fetch_hint.has_value());
    auto last_sn = cache.lastLogSequence();
    ASSERT_FALSE(last_sn.has_value());
}

TEST_F(LogEntryCacheNormalSizeTest, ClearAndReAppend)
{
    cache.clear();

    /// Verify that the cache is empty
    ASSERT_FALSE(cache.lastLogSequence().has_value());

    /// Re-append some entries
    for (int64_t i = 6; i <= 10; ++i)
    {
        auto entry = createEntry(i);
        cluster::nlog::LogSequenceMetadata metadata;
        metadata.segment_base_sn = 0;
        cluster::FetchHint fetch_hint;
        fetch_hint.sn = i + 1; /// Set fetch_hint.sn to the next sequence number
        cache.append(entry, metadata, fetch_hint);
    }

    /// Verify that the new entries are correctly added
    auto last_sn = cache.lastLogSequence();
    ASSERT_TRUE(last_sn.has_value());
    ASSERT_EQ(last_sn.value(), 10);
}
