#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateStreamSchemaResponseData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(UpdateStreamSchemaResponseDataTest, ConstructWithData)
{
    UpdateStreamSchemaResponseData data(0xBEEF, 42);
    EXPECT_EQ(data.replica_leader, 0xBEEF);
    EXPECT_EQ(data.sn, 42);
    EXPECT_FALSE(data.hasError());
}

TEST(UpdateStreamSchemaResponseDataTest, ConstructWithErrorCodeAndMessage)
{
    UpdateStreamSchemaResponseData data(123, "something went wrong");
    EXPECT_TRUE(data.hasError());
    EXPECT_EQ(data.errorString(), "error_code=123 error_message='something went wrong'");

}

TEST(UpdateStreamSchemaResponseDataTest, SerializeDeserializeRoundTrip)
{
    UpdateStreamSchemaResponseData original(0xAA, 98765);
    original.error() = cluster::Error(0, ""); 

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 2);

    DB::ReadBufferFromOwnString rb(wb.str());
    UpdateStreamSchemaResponseData copy;
    copy.deserialize(rb, 2);

    EXPECT_FALSE(copy.hasError());
    EXPECT_EQ(copy.replica_leader, 0xAA);
    EXPECT_EQ(copy.sn, 98765);
}

TEST(UpdateStreamSchemaResponseDataTest, SerializeDeserializeWithError)
{
    UpdateStreamSchemaResponseData original(cluster::Error(456, "decode error"));
    original.replica_leader = 999;
    original.sn = 8888;

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 2);

    DB::ReadBufferFromOwnString rb(wb.str());
    UpdateStreamSchemaResponseData copy;
    copy.deserialize(rb, 2);

    EXPECT_TRUE(copy.hasError());
    EXPECT_EQ(copy.errorString(), "error_code=456 error_message='decode error'");
    EXPECT_EQ(copy.replica_leader, 999);
    EXPECT_EQ(copy.sn, 8888);
}

TEST(UpdateStreamSchemaResponseDataTest, DoStringIncludesAllFields)
{
    UpdateStreamSchemaResponseData data(cluster::Error(0, ""));
    data.replica_leader = 0x1111;
    data.sn = 2222;

    EXPECT_FALSE(data.hasError());
    EXPECT_EQ(data.replica_leader, 0x1111);
    EXPECT_EQ(data.sn, 2222);

}

TEST(UpdateStreamSchemaResponseDataTest, ApproximateSizeIsNonZero)
{
    UpdateStreamSchemaResponseData data(0x55, 777);
    size_t size = data.approximateSerializedSize();
    EXPECT_GT(size, 0u);
}

TEST(UpdateStreamSchemaResponseDataTest, OpCodeAndVersionRange)
{
    UpdateStreamSchemaResponseData data;
    EXPECT_EQ(data.opCode(), OpCode::AlterStreamSchema);

    auto range = data.supportedVersionsRange();
    EXPECT_EQ(range.first, 1);
    EXPECT_EQ(range.second, 2);
}
