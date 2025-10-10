#include <Cluster/Protocol/CreateNamedCollectionRequestData.h>
#include <Cluster/Protocol/CreateNamedCollectionResponseData.h>
#include <Cluster/Protocol/DeleteNamedCollectionRequestData.h>
#include <Cluster/Protocol/DeleteNamedCollectionResponseData.h>
#include <Cluster/Protocol/GetNamedCollectionRequestData.h>
#include <Cluster/Protocol/GetNamedCollectionResponseData.h>
#include <Cluster/Protocol/ListNamedCollectionsRequestData.h>
#include <Cluster/Protocol/ListNamedCollectionsResponseData.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>


using namespace cluster;
using namespace cluster::protocol;

TEST(NamedCollectionDescriptorTest, SerializeDeserializeRoundTrip)
{
    NamedCollectionEntrys entries = {
        NamedCollectionEntry{.key = "key1", .value = "value1", .overridability = NamedCollectionOverridability::Default},
        NamedCollectionEntry{.key = "key2", .value = "value2", .overridability = NamedCollectionOverridability::Overridable},
        NamedCollectionEntry{.key = "key3", .value = "value3", .overridability = NamedCollectionOverridability::NotOverridable},
    };

    NamedCollectionDescriptor original(entries, /*data_version=*/42);
    original.create_timestamp_ms = 123456789;
    original.last_modify_timestamp_ms = 987654321;
    original.created_by = "alice";
    original.last_modified_by = "bob";

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);

    std::string serialized = wb.str();

    DB::ReadBufferFromString rb(serialized);
    NamedCollectionDescriptor copy;
    copy.deserialize(rb, 1);

    EXPECT_EQ(copy.data_version, 42u);
    EXPECT_EQ(copy.entries.size(), 3u);
    EXPECT_EQ(entries[0].key, "key1");
    EXPECT_EQ(entries[1].value, "value2");
    EXPECT_EQ(entries[2].overridability, NamedCollectionOverridability::NotOverridable);

    EXPECT_EQ(copy.create_timestamp_ms, 123456789);
    EXPECT_EQ(copy.last_modify_timestamp_ms, 987654321);
    EXPECT_EQ(copy.created_by, "alice");
    EXPECT_EQ(copy.last_modified_by, "bob");

    std::string str = copy.string();
    EXPECT_NE(str.find("42"), std::string::npos);
    EXPECT_NE(str.find("entries=3"), std::string::npos);
    EXPECT_NE(str.find("alice"), std::string::npos);
}

TEST(CreateNamedCollectionRequestDataTest, ParameterConstructorSetsFields)
{
    std::string collection_name{"my_collection"};
    NamedCollectionEntrys entries
        = {NamedCollectionEntry{.key = "keyA", .value = "valA", .overridability = NamedCollectionOverridability::Default}};
    auto descriptor = std::make_shared<NamedCollectionDescriptor>(entries, /*data_version=*/7);
    ExistsOperation exists_op = ExistsOperation::Ignore;
    std::string requested_by = "tester";
    cluster::NodeID initiator = 123;
    int64_t timeout = 5000;

    CreateNamedCollectionRequestData req(collection_name, std::move(descriptor), exists_op, requested_by, initiator, timeout);

    EXPECT_EQ(req.collection_name, "my_collection");
    EXPECT_EQ(req.collection->data_version, 7u);
    EXPECT_EQ(req.exists_op, exists_op);
    EXPECT_EQ(req.requested_by, "tester");
    EXPECT_EQ(req.initiator, initiator);
    EXPECT_EQ(req.timeout_ms, timeout);
    EXPECT_GT(req.requested_ts, 0);

    EXPECT_EQ(req.opCode(), protocol::OpCode::CreateNamedCollection);

    auto ver_range = req.supportedVersionsRange();
    EXPECT_EQ(ver_range.first, 1);
    EXPECT_EQ(ver_range.second, 1);
}

TEST(CreateNamedCollectionRequestDataTest, SerializeDeserializeRoundTrip)
{
    std::string collection_name{"user:42"};
    NamedCollectionEntrys entries = {
        NamedCollectionEntry{.key = "k1", .value = "v1", .overridability = NamedCollectionOverridability::Default},
        NamedCollectionEntry{.key = "k2", .value = "v2", .overridability = NamedCollectionOverridability::Overridable},
    };
    auto descriptor = std::make_shared<NamedCollectionDescriptor>(entries, /*data_version=*/88);
    ExistsOperation exists_op = ExistsOperation::Throw;
    std::string requested_by = "unit_test";
    NodeID initiator = 77;
    int64_t timeout = 12345;

    CreateNamedCollectionRequestData original(
        std::move(collection_name), std::move(descriptor), exists_op, requested_by, initiator, timeout);

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, /*version=*/1);
    std::string serialized = wb.str();

    EXPECT_LT(serialized.size(), original.approximateSerializedSize());

    DB::ReadBufferFromString rb(serialized);
    auto deserialized = std::make_shared<CreateNamedCollectionRequestData>();
    deserialized->deserialize(rb, /*version=*/1);

    // Check equality of key fields
    EXPECT_EQ(deserialized->collection_name, "user:42");
    EXPECT_EQ(deserialized->collection->entries.size(), 2u);
    EXPECT_EQ(deserialized->collection->entries[1].key, "k2");
    EXPECT_EQ(deserialized->collection->entries[1].overridability, NamedCollectionOverridability::Overridable);
    EXPECT_EQ(deserialized->collection->data_version, 88u);
    EXPECT_EQ(deserialized->exists_op, exists_op);
    EXPECT_EQ(deserialized->requested_by, requested_by);
    EXPECT_EQ(deserialized->initiator, initiator);
    EXPECT_EQ(deserialized->timeout_ms, timeout);
    EXPECT_EQ(deserialized->requested_ts, original.requested_ts);
}

TEST(CreateNamedCollectionResponseDataTest, ConstructorWithErrorCode)
{
    CreateNamedCollectionResponseData resp(42, "some error");
    EXPECT_TRUE(resp.hasError());
    EXPECT_EQ(resp.error().error_code, 42);
    EXPECT_EQ(resp.error().error_message, "some error");
}

TEST(CreateNamedCollectionResponseDataTest, ConstructorWithLeaderAndSn)
{
    NodeID leader = 123;
    int64_t sn = 4567;
    CreateNamedCollectionResponseData resp(leader, sn);
    EXPECT_FALSE(resp.hasError());
    EXPECT_EQ(resp.replica_leader, leader);
    EXPECT_EQ(resp.sn, sn);
}

TEST(CreateNamedCollectionResponseDataTest, SerializeDeserializeRoundTrip)
{
    {
        CreateNamedCollectionResponseData original(42, "error message");

        /// Serialize to memory buffer
        DB::WriteBufferFromOwnString wb;
        original.serialize(wb, 1);
        std::string serialized = wb.str();

        EXPECT_LT(serialized.size(), original.approximateSerializedSize());

        /// Deserialize into new object
        DB::ReadBufferFromString rb(serialized);
        CreateNamedCollectionResponseData deserialized;
        deserialized.deserialize(rb, 1);

        EXPECT_TRUE(deserialized.hasError());
        EXPECT_EQ(deserialized.error(), original.error());
    }

    {
        NodeID leader = 123;
        int64_t sn = 4567;
        CreateNamedCollectionResponseData original{leader, sn};

        /// Serialize to memory buffer
        DB::WriteBufferFromOwnString wb;
        original.serialize(wb, 1);
        std::string serialized = wb.str();

        EXPECT_LT(serialized.size(), original.approximateSerializedSize());

        /// Deserialize into new object
        DB::ReadBufferFromString rb(serialized);
        CreateNamedCollectionResponseData deserialized;
        deserialized.deserialize(rb, 1);

        EXPECT_FALSE(deserialized.hasError());
        EXPECT_EQ(deserialized.replica_leader, leader);
        EXPECT_EQ(deserialized.sn, sn);
    }
}

TEST(DeleteNamedCollectionRequestDataTest, ParameterConstructor)
{
    std::string collection_name = "my_collection";
    NodeID initiator = 42;
    int64_t timeout = 1000;

    DeleteNamedCollectionRequestData req(collection_name, initiator, timeout);
    EXPECT_EQ(req.collection_name, collection_name);
    EXPECT_EQ(req.initiator, initiator);
    EXPECT_EQ(req.timeout_ms, timeout);
}

TEST(DeleteNamedCollectionRequestDataTest, SerializeDeserializeRoundTrip)
{
    DeleteNamedCollectionRequestData original("user:99", 7, 5000);

    /// Serialize to memory buffer
    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    EXPECT_LT(serialized.size(), original.approximateSerializedSize());

    /// Deserialize into new object
    DB::ReadBufferFromString rb(serialized);
    DeleteNamedCollectionRequestData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_EQ(deserialized.collection_name, original.collection_name);
    EXPECT_EQ(deserialized.initiator, original.initiator);
    EXPECT_EQ(deserialized.timeout_ms, original.timeout_ms);
}

TEST(DeleteNamedCollectionResponseDataTest, ConstructorWithLeaderAndSn)
{
    NodeID leader = 42;
    int64_t sn = 100;
    DeleteNamedCollectionResponseData resp(leader, sn);

    EXPECT_EQ(resp.opCode(), protocol::OpCode::DeleteNamedCollection);
    EXPECT_FALSE(resp.hasError());
    EXPECT_EQ(resp.replica_leader, leader);
    EXPECT_EQ(resp.sn, sn);
}

TEST(DeleteNamedCollectionResponseDataTest, ConstructorWithErrorCode)
{
    DeleteNamedCollectionResponseData resp(7, "something went wrong");
    EXPECT_TRUE(resp.hasError());
    EXPECT_EQ(resp.error().error_code, 7);
    EXPECT_EQ(resp.error().error_message, "something went wrong");
}

TEST(DeleteNamedCollectionResponseDataTest, SerializeDeserializeRoundTrip)
{
    {
        DeleteNamedCollectionResponseData original(13, "error message");

        /// Serialize to memory buffer
        DB::WriteBufferFromOwnString wb;
        original.serialize(wb, 1);
        std::string serialized = wb.str();

        EXPECT_LT(serialized.size(), original.approximateSerializedSize());

        /// Deserialize into new object
        DB::ReadBufferFromString rb(serialized);
        DeleteNamedCollectionResponseData deserialized;
        deserialized.deserialize(rb, 1);

        EXPECT_TRUE(deserialized.hasError());
        EXPECT_EQ(deserialized.error(), original.error());
    }

    {
        NodeID leader = 24;
        int64_t sn = 123;
        DeleteNamedCollectionResponseData original(leader, sn);

        /// Serialize to memory buffer
        DB::WriteBufferFromOwnString wb;
        original.serialize(wb, 1);
        std::string serialized = wb.str();

        EXPECT_LT(serialized.size(), original.approximateSerializedSize());

        /// Deserialize into new object
        DB::ReadBufferFromString rb(serialized);
        DeleteNamedCollectionResponseData deserialized;
        deserialized.deserialize(rb, 1);

        EXPECT_FALSE(deserialized.hasError());
        EXPECT_EQ(deserialized.replica_leader, leader);
        EXPECT_EQ(deserialized.sn, sn);
    }
}

TEST(GetNamedCollectionRequestDataTest, ParameterConstructor)
{
    std::string name = "my_key";
    uint64_t versions = 5;
    NodeID initiator = 42;
    bool consistent_read = true;
    int64_t timeout = 2001;

    GetNamedCollectionRequestData req(name, versions, initiator, consistent_read, timeout);
    EXPECT_EQ(req.collection_name, name);
    EXPECT_EQ(req.versions_requested, versions);
    EXPECT_EQ(req.initiator, initiator);
    EXPECT_TRUE(req.consistent_read);
    EXPECT_EQ(req.timeout_ms, timeout);
}

TEST(GetNamedCollectionRequestDataTest, SerializeDeserializeRoundTrip)
{
    GetNamedCollectionRequestData original("user:99", 7, 5, true, 5000);

    /// Serialize to memory buffer
    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    EXPECT_LT(serialized.size(), original.approximateSerializedSize());

    /// Deserialize into new object
    DB::ReadBufferFromString rb(serialized);
    GetNamedCollectionRequestData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_EQ(deserialized.collection_name, original.collection_name);
    EXPECT_EQ(deserialized.versions_requested, original.versions_requested);
    EXPECT_EQ(deserialized.initiator, original.initiator);
    EXPECT_EQ(deserialized.consistent_read, original.consistent_read);
    EXPECT_EQ(deserialized.timeout_ms, original.timeout_ms);
}

TEST(GetNamedCollectionResponseDataTest, ConstructorWithErrorCode)
{
    GetNamedCollectionResponseData resp(42, "some error");
    EXPECT_EQ(resp.opCode(), protocol::OpCode::GetNamedCollection);
    EXPECT_TRUE(resp.hasError());
    EXPECT_EQ(resp.error().error_code, 42);
    EXPECT_EQ(resp.error().error_message, "some error");
}

TEST(GetNamedCollectionResponseDataTest, ConstructorWithValuesAndLeader)
{
    {
        NamedCollectionEntrys entries = {
            NamedCollectionEntry{.key = "alpha", .value = "a", .overridability = NamedCollectionOverridability::Default},
            NamedCollectionEntry{.key = "beta", .value = "b", .overridability = NamedCollectionOverridability::Overridable},
        };
        auto desc1 = std::make_shared<NamedCollectionDescriptor>(entries, /*data_version=*/5);
        auto desc2 = std::make_shared<NamedCollectionDescriptor>(entries, /*data_version=*/6);

        NamedCollectionDescriptorPtrs descs = {desc1, desc2};

        NodeID leader = 7;
        int64_t sn = 1234;

        GetNamedCollectionResponseData resp(std::move(descs), leader, sn);
        EXPECT_FALSE(resp.hasError());
        EXPECT_EQ(resp.named_collections.size(), 2u);
        EXPECT_EQ(resp.named_collections[0]->data_version, 5u);
        EXPECT_EQ(resp.named_collections[0]->entries[0].key, "alpha");
        EXPECT_EQ(resp.named_collections[0]->entries[0].value, "a");
        EXPECT_EQ(resp.named_collections[0]->entries[0].overridability, NamedCollectionOverridability::Default);
        EXPECT_EQ(resp.named_collections[1]->data_version, 6u);
        EXPECT_EQ(resp.named_collections[1]->entries[1].key, "beta");
        EXPECT_EQ(resp.named_collections[1]->entries[1].value, "b");
        EXPECT_EQ(resp.named_collections[1]->entries[1].overridability, NamedCollectionOverridability::Overridable);
        EXPECT_EQ(resp.replica_leader, leader);
        EXPECT_EQ(resp.sn, sn);
    }

    {
        GetNamedCollectionResponseData original(42, "some error");

        DB::WriteBufferFromOwnString wb;
        original.serialize(wb, 1); // version = 1
        std::string serialized = wb.str();

        DB::ReadBufferFromString rb(serialized);
        GetNamedCollectionResponseData deserialized;
        deserialized.deserialize(rb, 1);

        EXPECT_TRUE(deserialized.hasError());
        EXPECT_EQ(deserialized.error().error_code, 42);
        EXPECT_EQ(deserialized.error().error_message, "some error");

        EXPECT_TRUE(deserialized.named_collections.empty());

        EXPECT_EQ(deserialized.replica_leader, Nulls::NullNodeID);
        EXPECT_EQ(deserialized.sn, 0);
    }
}

TEST(ListNamedCollectionsRequestDataTest, ParameterConstructor)
{
    NodeID initiator = 42;
    bool consistent_read = true;
    int64_t timeout = 3000;

    ListNamedCollectionsRequestData req(initiator, consistent_read, timeout);
    EXPECT_EQ(req.opCode(), protocol::OpCode::ListNamedCollections);
    EXPECT_EQ(req.initiator, initiator);
    EXPECT_TRUE(req.consistent_read);
    EXPECT_EQ(req.timeout_ms, timeout);
}

TEST(ListNamedCollectionsRequestDataTest, SerializeDeserializeRoundTrip)
{
    ListNamedCollectionsRequestData original(7, true, 5000);

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    EXPECT_LT(serialized.size(), original.approximateSerializedSize());

    DB::ReadBufferFromString rb(serialized);
    ListNamedCollectionsRequestData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_EQ(deserialized.initiator, original.initiator);
    EXPECT_EQ(deserialized.consistent_read, original.consistent_read);
    EXPECT_EQ(deserialized.timeout_ms, original.timeout_ms);
}

TEST(ListNamedCollectionsResponseDataTest, ConstructorWithErrorCode)
{
    ListNamedCollectionsResponseData resp(42, "some error");
    EXPECT_EQ(resp.opCode(), protocol::OpCode::ListNamedCollections);
    EXPECT_TRUE(resp.hasError());
    EXPECT_EQ(resp.error().error_code, 42);
    EXPECT_EQ(resp.error().error_message, "some error");
}

TEST(ListNamedCollectionsResponseDataTest, ConstructorWithKeysAndLeader)
{
    std::vector<std::string> collection_names = {"key1", "key2"};
    NodeID leader = 7;
    int64_t sn = 1234;

    ListNamedCollectionsResponseData resp(std::move(collection_names), leader, sn);
    EXPECT_FALSE(resp.hasError());
    EXPECT_EQ(resp.collection_names.size(), 2u);
    EXPECT_EQ(resp.collection_names[0], "key1");
    EXPECT_EQ(resp.collection_names[1], "key2");
    EXPECT_EQ(resp.replica_leader, leader);
    EXPECT_EQ(resp.sn, sn);
}

TEST(ListNamedCollectionsResponseDataTest, SerializeDeserializeRoundTrip)
{
    std::vector<std::string> names = {"key1", "key2"};
    ListNamedCollectionsResponseData original(std::move(names), 7, 1234);

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    EXPECT_LT(serialized.size(), original.approximateSerializedSize());

    DB::ReadBufferFromString rb(serialized);
    ListNamedCollectionsResponseData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_FALSE(deserialized.hasError());
    EXPECT_EQ(deserialized.collection_names.size(), 2u);
    EXPECT_EQ(deserialized.collection_names[0], "key1");
    EXPECT_EQ(deserialized.collection_names[1], "key2");
    EXPECT_EQ(deserialized.replica_leader, original.replica_leader);
    EXPECT_EQ(deserialized.sn, original.sn);
}

TEST(ListNamedCollectionsResponseDataTest, SerializeDeserializeWithError)
{
    ListNamedCollectionsResponseData original(13, "error occurred");

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    DB::ReadBufferFromString rb(serialized);
    ListNamedCollectionsResponseData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_TRUE(deserialized.hasError());
    EXPECT_EQ(deserialized.error().error_code, 13);
    EXPECT_EQ(deserialized.error().error_message, "error occurred");

    EXPECT_TRUE(deserialized.collection_names.empty());
    EXPECT_EQ(deserialized.replica_leader, Nulls::NullNodeID);
    EXPECT_EQ(deserialized.sn, 0);
}
