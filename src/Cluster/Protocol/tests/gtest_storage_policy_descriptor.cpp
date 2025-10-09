#include <gtest/gtest.h>

#include <Cluster/Protocol/StoragePolicyDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster::protocol;

TEST(StoragePolicyDescriptorTest, SerializeDeserialize)
{
    VolumeDescriptor vol;
    vol.name = "volume1";
    vol.type = VolumeDescriptor::VolumeType::SingleDisk;
    vol.max_data_part_size = 2048;
    vol.max_data_part_size_ratio = 0.6;
    vol.perform_ttl_move_on_insert = false;
    vol.load_balancing = VolumeDescriptor::VolumeLoadBalancing::LeastUsed;
    vol.volume_priority = 3;
    vol.disk_names = {"disk1", "disk2", "disk3"};

    StoragePolicyDescriptor original;
    original.data_version = 5;
    original.name = "test_policy";
    original.move_factor = 0.75;
    original.volumes = {vol};
    original.create_timestamp_ms = 1650000000000;
    original.last_modify_timestamp_ms = 1650001111000;
    original.created_by = "alice";
    original.last_modified_by = "bob";

    std::string serialized;
    DB::WriteBufferFromString wb(serialized);
    original.serialize(wb, StoragePolicyDescriptor::schema_version);
    wb.finalize();

    StoragePolicyDescriptor deserialized;
    DB::ReadBufferFromString rb(serialized);
    deserialized.deserialize(rb, StoragePolicyDescriptor::schema_version);

    EXPECT_EQ(deserialized.data_version, 5u);
    EXPECT_EQ(deserialized.name, "test_policy");
    EXPECT_DOUBLE_EQ(deserialized.move_factor, 0.75);
    ASSERT_EQ(deserialized.volumes.size(), 1u);

    const auto & v = deserialized.volumes[0];
    EXPECT_EQ(v.name, "volume1");
    EXPECT_EQ(v.type, VolumeDescriptor::VolumeType::SingleDisk);
    EXPECT_EQ(v.max_data_part_size, 2048u);
    EXPECT_DOUBLE_EQ(v.max_data_part_size_ratio, 0.6);
    EXPECT_FALSE(v.perform_ttl_move_on_insert);
    EXPECT_EQ(v.load_balancing, VolumeDescriptor::VolumeLoadBalancing::LeastUsed);
    EXPECT_EQ(v.volume_priority, 3u);
    EXPECT_EQ(v.disk_names, std::vector<std::string>({"disk1", "disk2", "disk3"}));

    EXPECT_EQ(deserialized.create_timestamp_ms, 1650000000000);
    EXPECT_EQ(deserialized.last_modify_timestamp_ms, 1650001111000);
    EXPECT_EQ(deserialized.created_by, "alice");
    EXPECT_EQ(deserialized.last_modified_by, "bob");
}

TEST(StoragePolicyDescriptorTest, StringContainsExpectedFields)
{
    VolumeDescriptor vol;
    vol.name = "volX";
    vol.disk_names = {"d1", "d2"};

    StoragePolicyDescriptor policy;
    policy.data_version = 1;
    policy.name = "policyXYZ";
    policy.created_by = "u1";
    policy.last_modified_by = "u2";
    policy.create_timestamp_ms = 123456789;
    policy.last_modify_timestamp_ms = 987654321;
    policy.volumes = {vol};

    std::string out = policy.string();

    EXPECT_NE(out.find("policyXYZ"), std::string::npos);
    EXPECT_NE(out.find("volX"), std::string::npos);
    EXPECT_NE(out.find("d1"), std::string::npos);
    EXPECT_NE(out.find("created_by=u1"), std::string::npos);
    EXPECT_NE(out.find("last_modified_by=u2"), std::string::npos);
}
