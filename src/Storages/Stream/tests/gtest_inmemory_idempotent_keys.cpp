#include <gtest/gtest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/Stream/InMemoryIdempotentKeys.h>
#include <Common/Logger.h>

using namespace DB;

namespace
{
LoggerPtr makeTestLog()
{
    return getLogger("gtest_inmemory_idempotent_keys");
}
}

TEST(InMemoryIdempotentKeys, AddNewKeyReturnsTrue)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String k = "key1";
    EXPECT_TRUE(keys.add(1, k));
    EXPECT_EQ(keys.size(), 1u);
}

TEST(InMemoryIdempotentKeys, AddDuplicateKeyReturnsFalse)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String k1 = "key1";
    EXPECT_TRUE(keys.add(1, k1));

    /// Same key with a different sn → still duplicate
    String k1_dup = "key1";
    EXPECT_FALSE(keys.add(2, k1_dup));
    EXPECT_EQ(keys.size(), 1u);
}

TEST(InMemoryIdempotentKeys, AddEmptyKeyAlwaysTreatedAsNew)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    /// Empty key → always returns true, not inserted into index
    String empty1;
    String empty2;
    EXPECT_TRUE(keys.add(1, empty1));
    EXPECT_TRUE(keys.add(2, empty2));
    EXPECT_EQ(keys.size(), 0u);
}

TEST(InMemoryIdempotentKeys, EvictsOldestKeyWhenAtCapacity)
{
    constexpr size_t max_ids = 3;
    InMemoryIdempotentKeys keys(max_ids, makeTestLog());

    String k1 = "k1", k2 = "k2", k3 = "k3", k4 = "k4";
    keys.add(1, k1);
    keys.add(2, k2);
    keys.add(3, k3);
    EXPECT_EQ(keys.size(), 3u);

    /// Adding a 4th key evicts the oldest (k1)
    keys.add(4, k4);
    EXPECT_EQ(keys.size(), 3u);

    /// k1 was evicted — treated as new
    String k1_again = "k1";
    EXPECT_TRUE(keys.add(5, k1_again));

    /// k4 still present — duplicate
    String k4_dup = "k4";
    EXPECT_FALSE(keys.add(6, k4_dup));
}

TEST(InMemoryIdempotentKeys, RewindToDropsEntriesAboveMaxSn)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String a = "A", b = "B", c = "C";
    keys.add(10, a);
    keys.add(20, b);
    keys.add(30, c);
    EXPECT_EQ(keys.size(), 3u);

    keys.rewindTo(20);
    EXPECT_EQ(keys.size(), 2u);

    /// C was dropped → can be re-added; A/B still present.
    String c_again = "C";
    EXPECT_TRUE(keys.add(30, c_again));

    String a_dup = "A", b_dup = "B";
    EXPECT_FALSE(keys.add(99, a_dup));
    EXPECT_FALSE(keys.add(99, b_dup));
}

TEST(InMemoryIdempotentKeys, RewindToIsNoOpWhenMaxSnCoversAllEntries)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String a = "A", b = "B";
    keys.add(10, a);
    keys.add(20, b);

    keys.rewindTo(20);
    EXPECT_EQ(keys.size(), 2u);

    keys.rewindTo(100);
    EXPECT_EQ(keys.size(), 2u);
}

TEST(InMemoryIdempotentKeys, RewindToBelowAllEmptiesStore)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String a = "A", b = "B";
    keys.add(10, a);
    keys.add(20, b);

    keys.rewindTo(9);
    EXPECT_EQ(keys.size(), 0u);

    /// Both keys can be re-added after a full rewind.
    String a_again = "A", b_again = "B";
    EXPECT_TRUE(keys.add(10, a_again));
    EXPECT_TRUE(keys.add(20, b_again));
}

TEST(InMemoryIdempotentKeys, RewindToPreservesDequeOrderForSnapshotTo)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String a = "A", b = "B", c = "C";
    keys.add(10, a);
    keys.add(20, b);
    keys.add(30, c);

    keys.rewindTo(20);

    auto snap = keys.snapshotTo(20, 100);
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->size(), 2u);
    String a_dup = "A", b_dup = "B";
    EXPECT_FALSE(snap->add(99, a_dup));
    EXPECT_FALSE(snap->add(99, b_dup));
}

TEST(InMemoryIdempotentKeys, RewindToEnablesReAddOfDroppedKey)
{
    /// Without rewind, a same-key re-add returns false; after rewind the retry succeeds.
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String r1 = "r1", r2 = "r2";
    EXPECT_TRUE(keys.add(10, r1));
    EXPECT_TRUE(keys.add(11, r2));

    String r2_no_rewind = "r2";
    EXPECT_FALSE(keys.add(11, r2_no_rewind));

    EXPECT_EQ(keys.rewindTo(10), 1u);
    EXPECT_EQ(keys.size(), 1u);

    String r2_retry = "r2";
    EXPECT_TRUE(keys.add(11, r2_retry));
    EXPECT_EQ(keys.size(), 2u);
}

TEST(InMemoryIdempotentKeys, RewindToFlagsUpdatesWhenEntriesDropped)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    String k = "K";
    keys.add(5, k);

    std::string buf;
    {
        WriteBufferFromString wb(buf);
        keys.serialize(wb, 0);
    }
    EXPECT_FALSE(keys.hasUpdatesSinceLastSerialization());

    /// No-op rewind: no dropped entries, no flag flip.
    keys.rewindTo(5);
    EXPECT_FALSE(keys.hasUpdatesSinceLastSerialization());

    /// Dropping entries marks updates so the next commitBatch re-serializes.
    keys.rewindTo(4);
    EXPECT_TRUE(keys.hasUpdatesSinceLastSerialization());
}

TEST(InMemoryIdempotentKeys, HasUpdatesFlagLifecycle)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    EXPECT_FALSE(keys.hasUpdatesSinceLastSerialization());

    String k = "key1";
    keys.add(1, k);
    EXPECT_TRUE(keys.hasUpdatesSinceLastSerialization());
}

TEST(InMemoryIdempotentKeys, ApproximateSerializedSizeIncreasesWithKeys)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    size_t empty_size = keys.approximateSerializedSize();
    EXPECT_GT(empty_size, 0u);

    String k = "testkey";
    keys.add(1, k);

    EXPECT_GT(keys.approximateSerializedSize(), empty_size);
}

TEST(InMemoryIdempotentKeys, SerializeDeserializeRoundTrip)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    {
        String k1 = "alpha", k2 = "beta", k3 = "gamma";
        keys.add(1, k1);
        keys.add(2, k2);
        keys.add(3, k3);
    }

    EXPECT_EQ(keys.size(), 3u);
    EXPECT_TRUE(keys.hasUpdatesSinceLastSerialization());

    std::string buf;
    {
        WriteBufferFromString wb(buf);
        keys.serialize(wb, 0);
    }

    /// Serialization clears the updates flag
    EXPECT_FALSE(keys.hasUpdatesSinceLastSerialization());

    /// Deserialize into a fresh object
    InMemoryIdempotentKeys keys2(100, makeTestLog());
    {
        ReadBufferFromString rb(buf);
        keys2.deserialize(rb, 0);
    }

    EXPECT_EQ(keys2.size(), 3u);

    /// All original keys must be recognized as duplicates
    String a = "alpha", b = "beta", g = "gamma";
    EXPECT_FALSE(keys2.add(10, a));
    EXPECT_FALSE(keys2.add(11, b));
    EXPECT_FALSE(keys2.add(12, g));

    /// A truly new key should be accepted
    String new_key = "delta";
    EXPECT_TRUE(keys2.add(13, new_key));
}

TEST(InMemoryIdempotentKeys, DeserializeRestoresMaxIds)
{
    constexpr size_t original_max = 5;
    InMemoryIdempotentKeys keys(original_max, makeTestLog());

    {
        String k = "only_key";
        keys.add(1, k);
    }

    std::string buf;
    {
        WriteBufferFromString wb(buf);
        keys.serialize(wb, 0);
    }

    /// Start with a different max_ids; deserialize should restore the serialized max_ids
    InMemoryIdempotentKeys keys2(1000, makeTestLog());
    {
        ReadBufferFromString rb(buf);
        keys2.deserialize(rb, 0);
    }

    EXPECT_EQ(keys2.size(), 1u);
}

TEST(InMemoryIdempotentKeys, StringRepresentationContainsKeyData)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    {
        String k1 = "foo", k2 = "bar";
        keys.add(10, k1);
        keys.add(20, k2);
    }

    const std::string s = keys.string();
    EXPECT_FALSE(s.empty());
    EXPECT_NE(s.find("max_ids"), std::string::npos);
    EXPECT_NE(s.find("keys_num"), std::string::npos);
    EXPECT_NE(s.find("foo"), std::string::npos);
    EXPECT_NE(s.find("bar"), std::string::npos);
}

TEST(InMemoryIdempotentKeys, SnapshotToFiltersKeysByMaxSn)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    {
        String k1 = "sn1_key", k2 = "sn5_key", k3 = "sn10_key";
        keys.add(1, k1);
        keys.add(5, k2);
        keys.add(10, k3);
    }

    /// Snapshot up to sn=5 → only k1 (sn=1) and k2 (sn=5) included; k3 (sn=10) excluded
    auto snapshot = keys.snapshotTo(5, 100);
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->size(), 2u);

    String dup1 = "sn1_key", dup2 = "sn5_key", new3 = "sn10_key";
    EXPECT_FALSE(snapshot->add(99, dup1));
    EXPECT_FALSE(snapshot->add(99, dup2));
    EXPECT_TRUE(snapshot->add(99, new3)); /// sn10_key not in snapshot → accepted
}

TEST(InMemoryIdempotentKeys, SnapshotToWithNegativeMaxSnReturnsEmpty)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    {
        String k = "somekey";
        keys.add(1, k);
    }

    /// max_sn < LogStartSN (0) → empty snapshot
    auto snapshot = keys.snapshotTo(-1, 100);
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->size(), 0u);
}

TEST(InMemoryIdempotentKeys, ConstructFromExistingKeysDeduplicates)
{
    std::deque<IdempotentKey> existing_keys;
    existing_keys.emplace_back(1, "key_a");
    existing_keys.emplace_back(2, "key_b");

    InMemoryIdempotentKeys keys(100, existing_keys, makeTestLog());
    EXPECT_EQ(keys.size(), 2u);

    String dup_a = "key_a";
    EXPECT_FALSE(keys.add(10, dup_a));

    String new_key = "key_c";
    EXPECT_TRUE(keys.add(11, new_key));
}

TEST(InMemoryIdempotentKeys, SerializeEmptyObject)
{
    InMemoryIdempotentKeys keys(100, makeTestLog());

    std::string buf;
    {
        WriteBufferFromString wb(buf);
        keys.serialize(wb, 0);
    }

    EXPECT_FALSE(buf.empty()); /// At minimum max_ids and size=0 are written

    InMemoryIdempotentKeys keys2(50, makeTestLog());
    {
        ReadBufferFromString rb(buf);
        keys2.deserialize(rb, 0);
    }
    EXPECT_EQ(keys2.size(), 0u);
}
