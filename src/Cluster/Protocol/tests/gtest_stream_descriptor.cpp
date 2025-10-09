#include <gtest/gtest.h>
#include <Cluster/Protocol/StreamDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(StreamDescriptorTest, SerializeDeserialize)
{
    UInt128 uuid_value;
    uuid_value.items[0] = 0x1111222233334444ULL;
    uuid_value.items[1] = 0x5555666677778888ULL;
    DB::UUID stream_id{uuid_value};

    std::string ns = "test_ns";
    std::string stream_name = "test_stream";

    StreamDescriptor original(
        std::move(ns),
        std::move(stream_name),
        stream_id,
        3,  /// shards
        "CREATE STREAM ...",
        10  /// data_version
    );

    original.inmemory = true;
    original.codec = DB::CompressionMethodByte::LZ4;
    original.flush_ms = 100;
    original.flush_messages = 50;
    original.flush_bytes = 1024;
    original.retention_ms = 3600000;
    original.retention_bytes = 1048576;
    original.create_timestamp_ms = 1234567890;
    original.last_modify_timestamp_ms = 1234567999;
    original.created_by = "creator";
    original.last_modified_by = "modifier";

    std::string serialized_data;
    DB::WriteBufferFromString wb(serialized_data);
    original.serialize(wb, StreamDescriptor::schema_version);
    wb.finalize();

    StreamDescriptor deserialized;
    DB::ReadBufferFromString rb(serialized_data);
    deserialized.deserialize(rb, StreamDescriptor::schema_version);

    EXPECT_EQ(deserialized.data_version, 10u);
    EXPECT_EQ(deserialized.stream.ns, "test_ns");
    EXPECT_EQ(deserialized.stream.name, "test_stream");
    EXPECT_EQ(deserialized.stream.id, stream_id);
    EXPECT_EQ(deserialized.shards, 3u);
    EXPECT_EQ(deserialized.sql_def, "CREATE STREAM ...");
    EXPECT_EQ(deserialized.inmemory, true);
    EXPECT_EQ(deserialized.codec, DB::CompressionMethodByte::LZ4);
    EXPECT_EQ(deserialized.flush_ms, 100u);
    EXPECT_EQ(deserialized.flush_messages, 50u);
    EXPECT_EQ(deserialized.flush_bytes, 1024u);
    EXPECT_EQ(deserialized.retention_ms, 3600000u);
    EXPECT_EQ(deserialized.retention_bytes, 1048576u);
    EXPECT_EQ(deserialized.create_timestamp_ms, 1234567890);
    EXPECT_EQ(deserialized.last_modify_timestamp_ms, 1234567999);
    EXPECT_EQ(deserialized.created_by, "creator");
    EXPECT_EQ(deserialized.last_modified_by, "modifier");
}

TEST(StreamDescriptorTest, StringContainsExpectedFields)
{
    UInt128 uuid_value;
    uuid_value.items[0] = 0x9999888877776666ULL;
    uuid_value.items[1] = 0x5555444433332222ULL;
    DB::UUID stream_id{uuid_value};

    StreamDescriptor desc(
        "ns_string",
        "stream_string",
        stream_id,
        1,  /// shards
        "SQL_DEF",
        1  /// data_version
    );

    desc.created_by = "user1";
    desc.last_modified_by = "user2";

    std::string output = desc.string();

    EXPECT_NE(output.find("ns_string"), std::string::npos);
    EXPECT_NE(output.find("stream_string"), std::string::npos);
    EXPECT_NE(output.find("shards=1"), std::string::npos);
    EXPECT_NE(output.find("SQL_DEF"), std::string::npos);
    EXPECT_NE(output.find("user1"), std::string::npos);
    EXPECT_NE(output.find("user2"), std::string::npos);
}
