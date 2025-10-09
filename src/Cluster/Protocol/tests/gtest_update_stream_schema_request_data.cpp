#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateStreamSchemaRequestData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(UpdateStreamSchemaRequestDataTest, ConstructAndAccessFields)
{
    UpdateStreamSchemaRequestData request(
        "db1", "table1", cluster::StreamID(99),
        {"ALTER ADD COLUMN x UInt32"}, "CREATE STREAM table1",
        0x777, 5000, 0x42);

    EXPECT_EQ(request.stream.ns, "db1");
    EXPECT_EQ(request.stream.name, "table1");
    EXPECT_EQ(request.stream.id, cluster::StreamID(99));
    EXPECT_EQ(request.alter_commands.size(), 1);
    EXPECT_EQ(request.sql_def, "CREATE STREAM table1");
    EXPECT_EQ(request.initiator, 0x777);
    EXPECT_EQ(request.timeout_ms, 5000);
    EXPECT_EQ(request.version_before_update, 0x42);
}

TEST(UpdateStreamSchemaRequestDataTest, SerializeDeserialize)
{
    UpdateStreamSchemaRequestData original(
        "my_ns", "my_tbl", cluster::StreamID(123),
        {"ALTER DROP COLUMN col1", "ALTER ADD COLUMN col2 String"},
        "CREATE STREAM my_tbl (col2 String)",
        0x8888, 9999, 0x123);

    original.modified_by = "admin";
    original.last_modified = 123456789;

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 2);

    DB::ReadBufferFromOwnString rb(wb.str());
    UpdateStreamSchemaRequestData copy;
    copy.deserialize(rb, 2);

    EXPECT_EQ(copy.stream.ns, "my_ns");
    EXPECT_EQ(copy.stream.name, "my_tbl");
    EXPECT_EQ(copy.stream.id, cluster::StreamID(123));
    ASSERT_EQ(copy.alter_commands.size(), 2);
    EXPECT_EQ(copy.alter_commands[0], "ALTER DROP COLUMN col1");
    EXPECT_EQ(copy.sql_def, "CREATE STREAM my_tbl (col2 String)");
    EXPECT_EQ(copy.modified_by, "admin");
    EXPECT_EQ(copy.last_modified, 123456789);
    EXPECT_EQ(copy.initiator, 0x8888);
    EXPECT_EQ(copy.timeout_ms, 9999);
    EXPECT_EQ(copy.version_before_update, 0x123);
}

TEST(UpdateStreamSchemaRequestDataTest, DoStringContainsImportantFields)
{
    UpdateStreamSchemaRequestData request(
        "a", "b", cluster::StreamID(0xAA),
        {"ALTER x"}, "SQL_DEF", 0xABCD, 1111, 0x55);

    request.modified_by = "tester";
    request.last_modified = 123;

    std::string result = request.doString();
    EXPECT_NE(result.find("version_before_update=0x55"), std::string::npos);
    EXPECT_NE(result.find("SQL_DEF"), std::string::npos);
    EXPECT_NE(result.find("tester"), std::string::npos);
    EXPECT_NE(result.find("initiator=0xabcd"), std::string::npos);
}

TEST(UpdateStreamSchemaRequestDataTest, ApproximateSerializedSizeNonZero)
{
    UpdateStreamSchemaRequestData data(
        "x", "y", cluster::StreamID(1),
        {"ALTER something"}, "DDL",
        0x1, 2000, 0x999);

    size_t size = data.approximateSerializedSize();
    EXPECT_GT(size, 0u);
}

TEST(UpdateStreamSchemaRequestDataTest, OpCodeAndVersionRange)
{
    UpdateStreamSchemaRequestData data;
    EXPECT_EQ(data.opCode(), OpCode::AlterStreamSchema);

    auto version_range = data.supportedVersionsRange();
    EXPECT_EQ(version_range.first, 1);
    EXPECT_EQ(version_range.second, 3);
}
