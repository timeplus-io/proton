#include <gtest/gtest.h>

#include <Cluster/Protocol/UpdateAccessEntityRequestData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

using namespace cluster::protocol;

TEST(UpdateAccessEntityRequestData, BasicSerialization)
{
    UInt128 uuid_value = {0x5678, 0x1234};
    DB::UUID uuid(uuid_value);

    AccessEntityDescription entity(uuid, 42, R"({"user": "alice", "roles": ["admin"]})");

    UpdateAccessEntityRequestData request(0xdeadbeef, entity, 1234, 10000);

    std::string buffer_string;
    DB::WriteBufferFromString wb(buffer_string);
    request.serialize(wb, /*version*/ 1);

    DB::ReadBufferFromString rb(buffer_string);
    UpdateAccessEntityRequestData deserialized;
    deserialized.deserialize(rb, /*version*/ 1);

    EXPECT_EQ(deserialized.version_before_update, 0xdeadbeef);
    EXPECT_EQ(deserialized.new_entity.id, uuid);
    EXPECT_EQ(deserialized.new_entity.data_version, 42u);
    EXPECT_EQ(deserialized.new_entity.entity, R"({"user": "alice", "roles": ["admin"]})");
    EXPECT_EQ(deserialized.initiator, 1234u);
    EXPECT_EQ(deserialized.timeout_ms, 10000);
}
