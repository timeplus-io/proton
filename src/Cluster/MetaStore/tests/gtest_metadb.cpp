#include <Cluster/MetaStore/MetaDB.h>

#include <Common/Logger.h>

#include <gtest/gtest.h>

namespace
{
cluster::protocol::StreamDescriptor getStreamDescriptor(uint32_t version)
{
    cluster::protocol::StreamDescriptor desc;
    desc.stream.name = "test";
    desc.stream.ns = "test";
    desc.data_version = version;

    return desc;
}
}

namespace DB::ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

TEST(MetaDB, StreamCRUD)
{
    std::filesystem::remove_all("/tmp/metadb");
    std::filesystem::create_directories("/tmp/metadb");

    cluster::meta::MetaDB metadb("/tmp/metadb", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));

    auto desc_v1 = getStreamDescriptor(/*version=*/1);
    {
        {
            auto r = metadb.saveStream(desc_v1, /*applied_sn=*/cluster::AppliedSequence{1});
            ASSERT_FALSE(r.hasError());

            auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
            ASSERT_FALSE(result.hasError());
            ASSERT_EQ(*result.result, desc_v1);

            auto applied_sn = metadb.loadAppliedSequence();
            ASSERT_FALSE(applied_sn.hasError());
            ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));
        }

        {
            auto results = metadb.listStreams(desc_v1.stream.ns);
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 1);
            EXPECT_EQ(*results.result.front(), desc_v1);
        }

        {
            auto results = metadb.listStreams();
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 1);
            EXPECT_EQ(*results.result.front(), desc_v1);
        }
    }

    auto desc2 = desc_v1;
    desc2.stream.name += "2";
    {
        {
            auto r = metadb.saveStream(desc2, /*applied_sn=*/cluster::AppliedSequence{1});
            ASSERT_FALSE(r.hasError());

            auto result = metadb.getStream(desc2.stream.ns, desc2.stream.name);
            ASSERT_FALSE(result.hasError());
            ASSERT_EQ(*result.result, desc2);

            auto applied_sn = metadb.loadAppliedSequence();
            ASSERT_FALSE(applied_sn.hasError());
            ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));
        }

        {
            auto results = metadb.listStreams(desc2.stream.ns);
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 2);
            EXPECT_EQ(*results.result.front(), desc_v1);
            EXPECT_EQ(*results.result.back(), desc2);
        }

        {
            auto results = metadb.listStreams();
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 2);
            EXPECT_EQ(*results.result.front(), desc_v1);
            EXPECT_EQ(*results.result.back(), desc2);
        }
    }

    {
        {
            auto r = metadb.deleteStream(desc_v1.stream.ns, desc_v1.stream.name, /*applied_sn=*/cluster::AppliedSequence{1});
            ASSERT_FALSE(r.hasError());
            auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
            ASSERT_TRUE(result.hasError());
            ASSERT_EQ(result.err.error_code, DB::ErrorCodes::RESOURCE_NOT_FOUND);

            auto applied_sn = metadb.loadAppliedSequence();
            ASSERT_FALSE(applied_sn.hasError());
            ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));
        }

        {
            auto results = metadb.listStreams(desc_v1.stream.ns);
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 1);
            EXPECT_EQ(*results.result.back(), desc2);
        }

        {
            auto results = metadb.listStreams();
            ASSERT_FALSE(results.hasError());
            ASSERT_EQ(results.result.size(), 1);
            EXPECT_EQ(*results.result.back(), desc2);
        }
    }

    {
        {
            auto r = metadb.deleteStream(desc2.stream.ns, desc2.stream.name, /*applied_sn=*/cluster::AppliedSequence{1});
            ASSERT_FALSE(r.hasError());

            auto applied_sn = metadb.loadAppliedSequence();
            ASSERT_FALSE(applied_sn.hasError());
            ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

            auto result = metadb.getStream(desc2.stream.ns, desc2.stream.name);
            ASSERT_TRUE(result.hasError());
            ASSERT_EQ(result.err.error_code, DB::ErrorCodes::RESOURCE_NOT_FOUND);
        }

        {
            auto results = metadb.listStreams(desc_v1.stream.ns);
            ASSERT_FALSE(results.hasError());
            ASSERT_TRUE(results.result.empty());
        }

        {
            auto results = metadb.listStreams();
            ASSERT_FALSE(results.hasError());
            ASSERT_TRUE(results.result.empty());
        }
    }
}

TEST(MetaDB, PendingRequestCRUD)
{
    std::filesystem::remove_all("/tmp/metadb_pending");
    std::filesystem::create_directories("/tmp/metadb_pending");

    cluster::meta::MetaDB metadb("/tmp/metadb_pending", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));

    // Test save and iterate
    {
        // Save some pending requests
        ASSERT_FALSE(metadb.savePendingRequest(1, "request_data_1").hasError());
        ASSERT_FALSE(metadb.savePendingRequest(2, "request_data_2").hasError());
        ASSERT_FALSE(metadb.savePendingRequest(5, "request_data_5").hasError());
        ASSERT_FALSE(metadb.savePendingRequest(10, "request_data_10").hasError());

        // Iterate from 0 should return all
        auto result = metadb.iteratePendingRequestsAfter(0);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 4);
        EXPECT_EQ(result.result[0].first, 1);
        EXPECT_EQ(result.result[0].second, "request_data_1");
        EXPECT_EQ(result.result[1].first, 2);
        EXPECT_EQ(result.result[1].second, "request_data_2");
        EXPECT_EQ(result.result[2].first, 5);
        EXPECT_EQ(result.result[2].second, "request_data_5");
        EXPECT_EQ(result.result[3].first, 10);
        EXPECT_EQ(result.result[3].second, "request_data_10");

        // Iterate from 2 should return 5 and 10
        result = metadb.iteratePendingRequestsAfter(2);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 2);
        EXPECT_EQ(result.result[0].first, 5);
        EXPECT_EQ(result.result[1].first, 10);

        // Iterate from 10 should return empty
        result = metadb.iteratePendingRequestsAfter(10);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 0);
    }

    // Test delete
    {
        ASSERT_FALSE(metadb.deletePendingRequest(2).hasError());
        
        // Verify deletion
        auto result = metadb.iteratePendingRequestsAfter(0);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 3);
        EXPECT_EQ(result.result[0].first, 1);
        EXPECT_EQ(result.result[1].first, 5);
        EXPECT_EQ(result.result[2].first, 10);

        // Deleting non-existent should not error
        ASSERT_FALSE(metadb.deletePendingRequest(100).hasError());
    }

    // Test cleanup
    {
        // Cleanup up to and including 5
        ASSERT_FALSE(metadb.cleanupOldPendingRequests(5).hasError());
        
        // Should only have 10 left
        auto result = metadb.iteratePendingRequestsAfter(0);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 1);
        EXPECT_EQ(result.result[0].first, 10);

        // Cleanup all
        ASSERT_FALSE(metadb.cleanupOldPendingRequests(100).hasError());
        
        result = metadb.iteratePendingRequestsAfter(0);
        ASSERT_FALSE(result.hasError());
        ASSERT_EQ(result.result.size(), 0);
    }
}

TEST(MetaDB, PendingRequestCrashRecovery)
{
    std::filesystem::remove_all("/tmp/metadb_crash");
    std::filesystem::create_directories("/tmp/metadb_crash");

    // Simulate crash between persist and apply
    {
        cluster::meta::MetaDB metadb("/tmp/metadb_crash", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));
        
        // Save pending requests
        ASSERT_FALSE(metadb.savePendingRequest(1, "request_1").hasError());
        ASSERT_FALSE(metadb.savePendingRequest(2, "request_2").hasError());
        ASSERT_FALSE(metadb.savePendingRequest(3, "request_3").hasError());
        
        // Simulate apply of request 1
        ASSERT_FALSE(metadb.saveAppliedSequence(cluster::AppliedSequence{1}).hasError());
        ASSERT_FALSE(metadb.deletePendingRequest(1).hasError());
    }
    
    // Recovery after crash
    {
        cluster::meta::MetaDB metadb("/tmp/metadb_crash", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));
        
        // Load last applied
        auto applied_result = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_result.hasError());
        ASSERT_EQ(applied_result.result.sn, 1);
        
        // Iterate pending requests after last applied
        auto pending_result = metadb.iteratePendingRequestsAfter(applied_result.result.sn);
        ASSERT_FALSE(pending_result.hasError());
        ASSERT_EQ(pending_result.result.size(), 2);
        EXPECT_EQ(pending_result.result[0].first, 2);
        EXPECT_EQ(pending_result.result[0].second, "request_2");
        EXPECT_EQ(pending_result.result[1].first, 3);
        EXPECT_EQ(pending_result.result[1].second, "request_3");
    }
}

TEST(MetaDB, PendingRequestKeyFormat)
{
    // Test that keys are properly formatted and sorted
    std::filesystem::remove_all("/tmp/metadb_keyformat");
    std::filesystem::create_directories("/tmp/metadb_keyformat");

    cluster::meta::MetaDB metadb("/tmp/metadb_keyformat", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));

    // Save requests with various sequence numbers
    ASSERT_FALSE(metadb.savePendingRequest(1, "data_1").hasError());
    ASSERT_FALSE(metadb.savePendingRequest(99, "data_99").hasError());
    ASSERT_FALSE(metadb.savePendingRequest(1000, "data_1000").hasError());
    ASSERT_FALSE(metadb.savePendingRequest(99999, "data_99999").hasError());

    // Verify they are returned in correct order
    auto result = metadb.iteratePendingRequestsAfter(0);
    ASSERT_FALSE(result.hasError());
    ASSERT_EQ(result.result.size(), 4);
    EXPECT_EQ(result.result[0].first, 1);
    EXPECT_EQ(result.result[1].first, 99);
    EXPECT_EQ(result.result[2].first, 1000);
    EXPECT_EQ(result.result[3].first, 99999);
}

TEST(MetaDB, RenameStream)
{
    std::filesystem::remove_all("/tmp/metadb");
    std::filesystem::create_directories("/tmp/metadb");

    cluster::meta::MetaDB metadb("/tmp/metadb", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));

    auto desc_v1 = getStreamDescriptor(/*version=*/1);

    auto r = metadb.saveStream(desc_v1, /*applied_sn=*/cluster::AppliedSequence(1));
    ASSERT_FALSE(r.hasError());

    auto applied_sn = metadb.loadAppliedSequence();
    ASSERT_FALSE(applied_sn.hasError());
    ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

    auto desc_v2 = desc_v1;
    desc_v2.stream.name += "_renamed";
    r = metadb.renameStream(desc_v1.stream.name, desc_v2, cluster::AppliedSequence{1});
    ASSERT_FALSE(r.hasError());

    auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
    ASSERT_TRUE(result.hasError());
    ASSERT_EQ(result.err.error_code, DB::ErrorCodes::RESOURCE_NOT_FOUND);

    result = metadb.getStream(desc_v2.stream.ns, desc_v2.stream.name);
    ASSERT_FALSE(result.hasError());
    ASSERT_EQ(*result.result, desc_v2);
}

TEST(MetaDB, StreamMultiVersions)
{
    std::filesystem::remove_all("/tmp/metadb");
    std::filesystem::create_directories("/tmp/metadb");

    cluster::meta::MetaDB metadb("/tmp/metadb", cluster::StreamIDShard{}, /*metadata_keep_versions=*/3, getLogger("MetaDB"));

    auto desc_v1 = getStreamDescriptor(/*version=*/1);

    auto r = metadb.saveStream(desc_v1, /*applied_sn=*/cluster::AppliedSequence{1});
    ASSERT_FALSE(r.hasError());
    {
        auto applied_sn = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_sn.hasError());
        ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

        {
            auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);

            ASSERT_FALSE(result.hasError());
            EXPECT_EQ(*result.result, desc_v1);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/1);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 1);
            EXPECT_EQ(*result_v.result[0], desc_v1);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/2);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 1);
            EXPECT_EQ(*result_v.result[0], desc_v1);
        }
    }

    auto desc_v2 = getStreamDescriptor(/*version=*/2);
    metadb.saveStream(desc_v2, /*applied_sn=*/cluster::AppliedSequence{1});
    {
        auto applied_sn = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_sn.hasError());
        ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

        {
            auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
            ASSERT_FALSE(result.hasError());
            EXPECT_EQ(*result.result, desc_v2);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/1);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 1);
            EXPECT_EQ(*result_v.result[0], desc_v2);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/2);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 2);
            EXPECT_EQ(*result_v.result[0], desc_v1);
            EXPECT_EQ(*result_v.result[1], desc_v2);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/3);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 2);
            EXPECT_EQ(*result_v.result[0], desc_v1);
            EXPECT_EQ(*result_v.result[1], desc_v2);
        }
    }

    auto desc_v3 = getStreamDescriptor(/*version=*/3);
    metadb.saveStream(desc_v3, /*applied_sn=*/cluster::AppliedSequence{1});
    {
        auto applied_sn = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_sn.hasError());
        ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));
    }

    auto desc_v4 = getStreamDescriptor(/*version=*/3);
    metadb.saveStream(desc_v4, /*applied_sn=*/cluster::AppliedSequence{1});
    {
        auto applied_sn = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_sn.hasError());
        ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

        {
            auto result = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
            ASSERT_FALSE(result.hasError());
            EXPECT_EQ(*result.result, desc_v4);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/1);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 1);
            EXPECT_EQ(*result_v.result[0], desc_v4);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/2);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 2);
            EXPECT_EQ(*result_v.result[0], desc_v3);
            EXPECT_EQ(*result_v.result[1], desc_v4);
        }

        {
            auto result_v = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name, /*versions=*/3);
            ASSERT_FALSE(result_v.hasError());
            ASSERT_EQ(result_v.result.size(), 3);
            EXPECT_EQ(*result_v.result[0], desc_v2);
            EXPECT_EQ(*result_v.result[1], desc_v3);
            EXPECT_EQ(*result_v.result[2], desc_v4);
        }
    }

    {
        auto ns_streams = metadb.listStreams(desc_v1.stream.ns);
        ASSERT_FALSE(ns_streams.hasError());
        ASSERT_EQ(ns_streams.result.size(), 1);
        EXPECT_EQ(*ns_streams.result[0], desc_v4);

        auto streams = metadb.listStreams();
        ASSERT_FALSE(streams.hasError());
        ASSERT_EQ(streams.result.size(), 1);
        EXPECT_EQ(*streams.result[0], desc_v4);
    }

    auto err = metadb.deleteStream(desc_v1.stream.ns, desc_v1.stream.name, /*applied_sn=*/cluster::AppliedSequence{1});
    ASSERT_FALSE(err.hasError());
    {
        auto applied_sn = metadb.loadAppliedSequence();
        ASSERT_FALSE(applied_sn.hasError());
        ASSERT_EQ(applied_sn.result, cluster::AppliedSequence(1));

        auto ns_streams = metadb.listStreams(desc_v1.stream.ns);
        ASSERT_FALSE(ns_streams.hasError());
        ASSERT_TRUE(ns_streams.result.empty());

        auto streams = metadb.listStreams();
        ASSERT_FALSE(streams.hasError());
        ASSERT_TRUE(streams.result.empty());

        auto stream = metadb.getStream(desc_v1.stream.ns, desc_v1.stream.name);
        ASSERT_TRUE(stream.hasError());
        ASSERT_EQ(stream.err.error_code, DB::ErrorCodes::RESOURCE_NOT_FOUND);
    }
}

