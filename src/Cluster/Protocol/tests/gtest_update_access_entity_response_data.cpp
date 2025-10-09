#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateAccessEntityResponseData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(UpdateAccessEntityResponseDataTest, ConstructWithErrorCodeAndMessage)
{
    UpdateAccessEntityResponseData data(123, "access error");
    EXPECT_TRUE(data.hasError());
    EXPECT_EQ(data.errorString(), "error_code=123 error_message='access error'");
}

TEST(UpdateAccessEntityResponseDataTest, ConstructWithErrorObject)
{
    cluster::Error err(456, "entity not found");
    UpdateAccessEntityResponseData data(std::move(err));
    EXPECT_TRUE(data.hasError());
    EXPECT_EQ(data.errorString(), "error_code=456 error_message='entity not found'");
}

TEST(UpdateAccessEntityResponseDataTest, ConstructWithoutError)
{
    UpdateAccessEntityResponseData data(0x1111, 777);
    EXPECT_FALSE(data.hasError());
    EXPECT_EQ(data.replica_leader, 0x1111);
    EXPECT_EQ(data.sn, 777);
}

TEST(UpdateAccessEntityResponseDataTest, SerializeDeserializeWithError)
{
    UpdateAccessEntityResponseData original(999, "decode error");

    std::string buffer_str;
    {
        DB::WriteBufferFromString wb(buffer_str);
        original.serialize(wb, /*version*/ 1);
    }

    UpdateAccessEntityResponseData copy;
    {
        DB::ReadBufferFromString rb(buffer_str);
        copy.deserialize(rb, /*version*/ 1);
    }

    EXPECT_TRUE(copy.hasError());
    EXPECT_EQ(copy.errorString(), "error_code=999 error_message='decode error'");
}

TEST(UpdateAccessEntityResponseDataTest, SerializeDeserializeWithoutError)
{
    UpdateAccessEntityResponseData original(0xABCD, 8888);

    std::string buffer_str;
    {
        DB::WriteBufferFromString wb(buffer_str);
        original.serialize(wb, /*version*/ 1);
    }

    UpdateAccessEntityResponseData copy;
    {
        DB::ReadBufferFromString rb(buffer_str);
        copy.deserialize(rb, /*version*/ 1);
    }

    EXPECT_FALSE(copy.hasError());
    EXPECT_EQ(copy.replica_leader, 0xABCD);
    EXPECT_EQ(copy.sn, 8888);
}

TEST(UpdateAccessEntityResponseDataTest, StringifyOutput)
{
    UpdateAccessEntityResponseData data(1234, "access err");
    data.replica_leader = 0xDEAD;
    data.sn = 2024;

    std::string str = data.string();
    EXPECT_NE(str.find("error_code=1234"), std::string::npos);
    EXPECT_NE(str.find("replica_leader=0xdead"), std::string::npos);
    EXPECT_NE(str.find("sn=2024"), std::string::npos);
}

TEST(UpdateAccessEntityResponseDataTest, ApproximateSerializedSize)
{
    UpdateAccessEntityResponseData data_no_err(0x1, 100);
    EXPECT_GT(data_no_err.approximateSerializedSize(), 0);

    UpdateAccessEntityResponseData data_with_err(1234, "error message test");
    EXPECT_GT(data_with_err.approximateSerializedSize(), 0);
}
