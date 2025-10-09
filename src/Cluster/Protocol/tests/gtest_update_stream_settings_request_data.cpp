#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateStreamSettingsRequestData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>

using namespace cluster::protocol;

TEST(UpdateStreamSettingsRequestDataTest, ConstructAndAccessFields)
{
    std::unordered_map<std::string, int32_t> flush_settings = {{"flush_interval", 100}};
    std::unordered_map<std::string, int64_t> retention_settings = {{"retention_days", 7}};
    std::vector<std::string> alter_commands = {"ALTER SET foo = 1", "ALTER RESET bar"};

    UpdateStreamSettingsRequestData request(
        "default",
        "my_stream",
        cluster::StreamID(42),
        123,
        std::move(flush_settings),
        std::move(retention_settings),
        std::move(alter_commands),
        /*initiator=*/0xABCD,
        /*timeout_ms=*/5000);

    EXPECT_EQ(request.version_before_update, 123);
    EXPECT_EQ(request.stream.ns, "default");
    EXPECT_EQ(request.stream.name, "my_stream");
    EXPECT_EQ(request.stream.id, cluster::StreamID(42));
    EXPECT_EQ(request.flush_settings.at("flush_interval"), 100);
    EXPECT_EQ(request.retention_settings.at("retention_days"), 7);
    EXPECT_EQ(request.alter_commands.size(), 2);
    EXPECT_EQ(request.initiator, 0xABCD);
    EXPECT_EQ(request.timeout_ms, 5000);
}

TEST(UpdateStreamSettingsRequestDataTest, SerializeDeserialize)
{
    UpdateStreamSettingsRequestData original(
        "ns1",
        "stream1",
        cluster::StreamID(777),
        0x11,
        "CREATE STREAM stream1",
        {{"flush_level", 1}},
        {{"retention_hours", 24}},
        {"ALTER SET foo = 123"},
        0xFF,
        60000);

    original.modified_by = "test_user";
    original.last_modified = 123456789;

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, /*version*/ 3);

    DB::ReadBufferFromOwnString rb(wb.str());

    UpdateStreamSettingsRequestData copy;
    copy.deserialize(rb, 3);

    EXPECT_EQ(copy.version_before_update, 0x11);
    EXPECT_EQ(copy.stream.ns, "ns1");
    EXPECT_EQ(copy.stream.name, "stream1");
    EXPECT_EQ(copy.stream.id, cluster::StreamID(777));
    EXPECT_EQ(copy.sql_def, "CREATE STREAM stream1");
    EXPECT_EQ(copy.flush_settings.at("flush_level"), 1);
    EXPECT_EQ(copy.retention_settings.at("retention_hours"), 24);
    ASSERT_EQ(copy.alter_commands.size(), 1);
    EXPECT_EQ(copy.alter_commands[0], "ALTER SET foo = 123");
    EXPECT_EQ(copy.modified_by, "test_user");
    EXPECT_EQ(copy.last_modified, 123456789);
    EXPECT_EQ(copy.initiator, 0xFF);
    EXPECT_EQ(copy.timeout_ms, 60000);
}

TEST(UpdateStreamSettingsRequestDataTest, DoStringFormat)
{
    UpdateStreamSettingsRequestData data(
        "ns", "stream", cluster::StreamID(1), 1, "DDL SQL",
        {}, {}, {"ALTER foo"}, 0x12, 1000);

    std::string str = data.doString();
    EXPECT_NE(str.find("version=0x1"), std::string::npos);
    EXPECT_NE(str.find("flags=0x0"), std::string::npos);
    EXPECT_NE(str.find("ALTER foo"), std::string::npos);
    EXPECT_NE(str.find("sql_def=DDL SQL"), std::string::npos);
}

TEST(UpdateStreamSettingsRequestDataTest, ApproximateSerializedSizeNonZero)
{
    UpdateStreamSettingsRequestData data(
        "ns", "stream", cluster::StreamID(9), 2, "SQL",
        {{"flush_setting", 99}}, {{"retention", 12345}},
        {"ALTER x"}, 1, 2000);

    size_t size = data.approximateSerializedSize();
    EXPECT_GT(size, 0u);
}

TEST(UpdateStreamSettingsRequestDataTest, OpCodeAndVersionRange)
{
    UpdateStreamSettingsRequestData data;
    EXPECT_EQ(data.opCode(), OpCode::AlterStreamSettings);

    auto range = data.supportedVersionsRange();
    EXPECT_EQ(range.first, 1);
    EXPECT_EQ(range.second, 3);
}
