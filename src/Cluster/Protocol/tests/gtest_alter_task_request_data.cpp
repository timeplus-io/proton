#include <gtest/gtest.h>

#include <Cluster/Protocol/AlterTaskRequestData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(AlterTaskRequestDataTest, SerializeDeserialize)
{
    AlterTaskRequestData original(
        "default_ns",
        "task_name",
        DB::UUIDHelpers::generateV4(),
        1,
        AlterTaskRequestData::AlterType::STATUS,
        TaskStatus::Disabled,
        "alice",
        0xdeadbeef,
        60000);

    std::string buffer_str;
    DB::WriteBufferFromString wb(buffer_str);
    original.serialize(wb, 1);

    DB::ReadBufferFromString rb(buffer_str);
    AlterTaskRequestData deserialized;
    deserialized.deserialize(rb, 1);

    EXPECT_EQ(deserialized.ns, original.ns);
    EXPECT_EQ(deserialized.name, original.name);
    EXPECT_EQ(deserialized.id, original.id);
    EXPECT_EQ(deserialized.data_version, original.data_version);
    EXPECT_EQ(deserialized.type, original.type);
    EXPECT_EQ(deserialized.status, original.status);
    EXPECT_EQ(deserialized.initiator, original.initiator);
    EXPECT_EQ(deserialized.timeout_ms, original.timeout_ms);
    EXPECT_EQ(deserialized.requested_by, original.requested_by);
    EXPECT_EQ(deserialized.requested_ts, original.requested_ts);

    std::string do_str = deserialized.doString();
    EXPECT_NE(do_str.find("status=Disabled"), std::string::npos);
    EXPECT_NE(do_str.find("ns=default_ns"), std::string::npos);
    EXPECT_NE(do_str.find("name=task_name"), std::string::npos);
    EXPECT_NE(do_str.find("requested_by=alice"), std::string::npos);
}
