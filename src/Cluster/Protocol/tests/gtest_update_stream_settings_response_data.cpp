#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateStreamSettingsResponseData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(UpdateStreamSettingsResponseDataTest, ConstructAndAccessors)
{
    UpdateStreamSettingsResponseData data(cluster::NodeID(123), 456);

    EXPECT_EQ(data.replica_leader, 123);
    EXPECT_EQ(data.sn, 456);
    EXPECT_FALSE(data.hasError());
}

TEST(UpdateStreamSettingsResponseDataTest, ErrorConstructor)
{
    UpdateStreamSettingsResponseData error_data(500, "Internal error");

    EXPECT_TRUE(error_data.hasError());
    EXPECT_EQ(error_data.error().error_code, 500);
    EXPECT_EQ(error_data.error().error_message, "Internal error");
    EXPECT_NE(error_data.errorString().find("Internal error"), std::string::npos);
}

TEST(UpdateStreamSettingsResponseDataTest, ErrorObjectConstructor)
{
    cluster::Error err(403, "Forbidden");
    UpdateStreamSettingsResponseData data(std::move(err));

    EXPECT_TRUE(data.hasError());
    EXPECT_EQ(data.error().error_code, 403);
    EXPECT_EQ(data.error().error_message, "Forbidden");
}

TEST(UpdateStreamSettingsResponseDataTest, SerializeDeserialize)
{
    UpdateStreamSettingsResponseData original(cluster::NodeID(321), 789);

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, /*version*/ 2);

    DB::ReadBufferFromOwnString rb(wb.str());
    UpdateStreamSettingsResponseData deserialized;
    deserialized.deserialize(rb, 2);

    EXPECT_EQ(deserialized.replica_leader, 321);
    EXPECT_EQ(deserialized.sn, 789);
    EXPECT_FALSE(deserialized.hasError());
}

TEST(UpdateStreamSettingsResponseDataTest, DoStringOutput)
{
    UpdateStreamSettingsResponseData data(cluster::NodeID(7), 999);
    std::string str = data.doString();

    EXPECT_NE(str.find("replica_leader=7"), std::string::npos);
    EXPECT_NE(str.find("sn=999"), std::string::npos);
}

TEST(UpdateStreamSettingsResponseDataTest, ApproximateSize)
{
    UpdateStreamSettingsResponseData data(cluster::NodeID(88), 1000);
    size_t size = data.approximateSerializedSize();

    EXPECT_GT(size, 0u);
}

TEST(UpdateStreamSettingsResponseDataTest, SupportedVersionsRange)
{
    UpdateStreamSettingsResponseData data;
    auto range = data.supportedVersionsRange();

    EXPECT_EQ(range.first, 1);
    EXPECT_EQ(range.second, 2);
}

TEST(UpdateStreamSettingsResponseDataTest, OpCodeCheck)
{
    UpdateStreamSettingsResponseData data;
    EXPECT_EQ(data.opCode(), OpCode::AlterStreamSettings);
}
