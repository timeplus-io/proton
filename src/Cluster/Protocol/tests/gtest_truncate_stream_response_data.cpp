#include <gtest/gtest.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Cluster/Protocol/TruncateStreamResponseData.h>

using namespace cluster::protocol;

TEST(TruncateStreamResponseDataTest, SerializeAndDeserializeWithError)
{
    TruncateStreamResponseData original(1234, "truncate failed");

    std::string buffer_str;
    DB::WriteBufferFromString wb(buffer_str);
    original.serialize(wb, 1);

    DB::ReadBufferFromString rb(buffer_str);
    TruncateStreamResponseData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_TRUE(deserialized.hasError());
    EXPECT_EQ(deserialized.errorString(), "error_code=1234 error_message='truncate failed'");
    EXPECT_EQ(deserialized.opCode(), OpCode::TruncateStream);
}

TEST(TruncateStreamResponseDataTest, SerializeAndDeserializeWithoutError)
{
    TruncateStreamResponseData original;

    std::string buffer_str;
    DB::WriteBufferFromString wb(buffer_str);
    original.serialize(wb, 1);

    DB::ReadBufferFromString rb(buffer_str);
    TruncateStreamResponseData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_FALSE(deserialized.hasError());
    EXPECT_EQ(deserialized.errorString(), "error_code=0 error_message='OK'");
}
