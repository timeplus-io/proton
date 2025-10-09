#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaDB.h>

#include <Common/Logger.h>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int SYSTEM_ERROR;
}

class LocalMetaQueueTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a unique test directory for each test
        test_dir = "/tmp/local_meta_queue_test_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        // Create MetaDB
        meta_db = std::make_shared<cluster::meta::MetaDB>(
            test_dir,
            cluster::StreamIDShard{},
            /*metadata_keep_versions=*/3,
            getLogger("MetaDB"));
    }

    void TearDown() override { std::filesystem::remove_all(test_dir); }

    std::string test_dir;
    std::shared_ptr<cluster::meta::MetaDB> meta_db;
};

TEST_F(LocalMetaQueueTest, BasicAppendAndFetch)
{
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

    // Append some data
    auto result1 = queue->append("data1");
    ASSERT_FALSE(result1.hasError());
    EXPECT_EQ(result1.result, 1);

    auto result2 = queue->append("data2");
    ASSERT_FALSE(result2.hasError());
    EXPECT_EQ(result2.result, 2);

    // Fetch from beginning
    auto fetch_result = queue->fetch(1, 1024, 0);
    ASSERT_FALSE(fetch_result.hasError());
    ASSERT_EQ(fetch_result.result->entries.size(), 2);
    EXPECT_EQ(fetch_result.result->entries[0]->sn, 1);
    EXPECT_EQ(fetch_result.result->entries[0]->data.string(), "data1");
    EXPECT_EQ(fetch_result.result->entries[1]->sn, 2);
    EXPECT_EQ(fetch_result.result->entries[1]->data.string(), "data2");
    // Note: next_sn doesn't exist in FetchResult, check if there's an alternative
}

TEST_F(LocalMetaQueueTest, ContiguousFetch)
{
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

    // Append data
    queue->append("data1"); // sn=1
    queue->append("data2"); // sn=2
    queue->append("data3"); // sn=3
    queue->append("data4"); // sn=4

    // Erase up to sn=2 to remove entries 1 and 2
    queue->eraseUpTo(2);

    // Now queue has [3,4] which are contiguous
    auto fetch_result = queue->fetch(3, 1024, 0);
    ASSERT_FALSE(fetch_result.hasError());
    // Should fetch both 3 and 4 as they are contiguous
    ASSERT_EQ(fetch_result.result->entries.size(), 2);
    EXPECT_EQ(fetch_result.result->entries[0]->sn, 3);
    EXPECT_EQ(fetch_result.result->entries[1]->sn, 4);
}

TEST_F(LocalMetaQueueTest, Backpressure)
{
    // Create queue with small limits
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(
        meta_db,
        getLogger("LocalMetaQueue"),
        /*max_entries=*/2,
        /*max_bytes=*/100);

    // Fill queue
    queue->append("data1");
    queue->append("data2");

    // Third append should block
    std::atomic<bool> append_completed{false};
    std::thread appender([&]() {
        auto result = queue->append("data3");
        append_completed = true;
        EXPECT_FALSE(result.hasError());
    });

    // Give time for append to block
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(append_completed.load());

    // Erase to make space
    queue->eraseUpTo(1);

    // Now append should complete
    appender.join();
    EXPECT_TRUE(append_completed.load());
}

TEST_F(LocalMetaQueueTest, EraseUpTo)
{
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

    // Append data
    for (int i = 1; i <= 5; ++i)
    {
        queue->append("data" + std::to_string(i));
    }

    EXPECT_EQ(queue->size(), 5);

    // Erase up to 3
    queue->eraseUpTo(3);
    EXPECT_EQ(queue->size(), 2);

    // Fetch should start from 4 (the first available entry)
    auto fetch_result = queue->fetch(4, 1024, 0);
    ASSERT_FALSE(fetch_result.hasError());
    ASSERT_EQ(fetch_result.result->entries.size(), 2);
    EXPECT_EQ(fetch_result.result->entries[0]->sn, 4);
    EXPECT_EQ(fetch_result.result->entries[1]->sn, 5);
}

TEST_F(LocalMetaQueueTest, Recovery)
{
    // Phase 1: Create queue and append data
    {
        auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

        queue->append("data1");
        queue->append("data2");
        queue->append("data3");

        // Simulate apply of first entry
        meta_db->saveAppliedSequence(cluster::AppliedSequence{1});
        meta_db->deletePendingRequest(1);
    }

    // Phase 2: Recover and verify
    {
        auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

        auto result = queue->recoverFromPersistent(1);
        ASSERT_FALSE(result.hasError());

        // Should have recovered entries 2 and 3
        EXPECT_EQ(queue->size(), 2);
        EXPECT_EQ(queue->nextSequenceNumber(), 4);

        auto fetch_result = queue->fetch(2, 1024, 0);
        ASSERT_FALSE(fetch_result.hasError());
        ASSERT_EQ(fetch_result.result->entries.size(), 2);
        EXPECT_EQ(fetch_result.result->entries[0]->sn, 2);
        EXPECT_EQ(fetch_result.result->entries[0]->data.string(), "data2");
        EXPECT_EQ(fetch_result.result->entries[1]->sn, 3);
        EXPECT_EQ(fetch_result.result->entries[1]->data.string(), "data3");
    }
}

TEST_F(LocalMetaQueueTest, BlockingFetch)
{
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

    std::atomic<bool> fetch_completed{false};
    std::shared_ptr<cluster::nlog::FetchResult> fetch_result;

    // Start fetch that will block
    std::thread fetcher([&]() {
        auto result = queue->fetch(1, 1024, 1000); // 1 second timeout
        EXPECT_FALSE(result.hasError());
        fetch_result = result.result;
        fetch_completed = true;
    });

    // Give time for fetch to start waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(fetch_completed.load());

    // Append data to unblock
    queue->append("data1");

    fetcher.join();
    EXPECT_TRUE(fetch_completed.load());
    ASSERT_EQ(fetch_result->entries.size(), 1);
    EXPECT_EQ(fetch_result->entries[0]->data.string(), "data1");
}

TEST_F(LocalMetaQueueTest, Shutdown)
{
    auto queue = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"));

    queue->append("data1");

    // Shutdown
    queue->shutdown();

    // Append after shutdown should fail
    auto result = queue->append("data2");
    EXPECT_TRUE(result.hasError());
    EXPECT_EQ(result.err.error_code, DB::ErrorCodes::SYSTEM_ERROR);

    // Fetch after shutdown should return empty
    auto fetch_result = queue->fetch(1, 1024, 0);
    EXPECT_FALSE(fetch_result.hasError());
    EXPECT_TRUE(fetch_result.result->entries.empty());
}