#include <gtest/gtest.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

using namespace cluster::protocol;

TEST(StoragePolicyDescriptorTest, SerializeDeserialize)
{
    VolumeDescriptor vol;
    vol.name = "volume1";
    vol.type = VolumeDescriptor::VolumeType::SingleDisk;
    vol.max_data_part_size = 2048;
    vol.max_data_part_size_ratio = 0.6;
    vol.perform_ttl_move_on_insert = false;
    vol.prefer_not_to_merge = true;
    vol.load_balancing = VolumeDescriptor::VolumeLoadBalancing::LeastUsed;
    vol.volume_priority = 3;
    vol.least_used_ttl_ms = 5000;
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
    EXPECT_TRUE(v.prefer_not_to_merge);
    EXPECT_EQ(v.load_balancing, VolumeDescriptor::VolumeLoadBalancing::LeastUsed);
    EXPECT_EQ(v.volume_priority, 3u);
    EXPECT_EQ(v.least_used_ttl_ms, 5000u);
    EXPECT_EQ(v.disk_names, std::vector<std::string>({"disk1", "disk2", "disk3"}));

    EXPECT_EQ(deserialized.create_timestamp_ms, 1650000000000);
    EXPECT_EQ(deserialized.last_modify_timestamp_ms, 1650001111000);
    EXPECT_EQ(deserialized.created_by, "alice");
    EXPECT_EQ(deserialized.last_modified_by, "bob");
}

TEST(StoragePolicyDescriptorTest, DeserializeV1PayloadAcceptsAndDefaultsNewFields)
{
    /// Hand-build a v1 wire payload (no prefer_not_to_merge / least_used_ttl_ms tail) and
    /// verify that the current (v2) deserialize reads it without misaligning the buffer.
    /// The v2-only fields should land at their struct defaults.
    constexpr uint32_t kWireSchemaV1 = 1;
    constexpr uint32_t kDataVersion = 9;

    std::string buf;
    {
        DB::WriteBufferFromString wb(buf);

        /// StoragePolicyDescriptor header: schema_data_version = (schema<<32) | data
        uint64_t schema_data_version = (static_cast<uint64_t>(kWireSchemaV1) << 32) + kDataVersion;
        DB::writeVarUInt(schema_data_version, wb);

        DB::writeStringBinary(std::string("legacy_policy"), wb);
        DB::writeBinary(0.42, wb);

        /// One volume — v1 fields only, no trailing prefer_not_to_merge / least_used_ttl_ms.
        DB::writeVarUInt(static_cast<uint64_t>(1), wb);  // serializeSerializables size prefix

        DB::writeStringBinary(std::string("legacy_vol"), wb);
        /// type — raw 1-byte write via serializeEnum -> writeIntBinary
        DB::writeIntBinary(static_cast<uint8_t>(VolumeDescriptor::VolumeType::JBOD), wb);
        DB::writeVarUInt(static_cast<uint64_t>(4096), wb);  // max_data_part_size
        DB::writeBinary(0.5, wb);                            // max_data_part_size_ratio
        DB::writeBinary(true, wb);                           // perform_ttl_move_on_insert
        /// load_balancing — raw 1-byte write via serializeEnum -> writeIntBinary
        DB::writeIntBinary(static_cast<uint8_t>(VolumeDescriptor::VolumeLoadBalancing::RoundRobin), wb);
        DB::writeVarUInt(static_cast<uint64_t>(7), wb);      // volume_priority

        /// disk_names via cluster::serialize (size prefix + each string)
        std::vector<std::string> disk_names = {"d_a", "d_b"};
        cluster::serialize(disk_names, wb);

        /// StoragePolicyDescriptor tail
        DB::writeVarInt(static_cast<int64_t>(111), wb);  // create_timestamp_ms
        DB::writeVarInt(static_cast<int64_t>(222), wb);  // last_modify_timestamp_ms
        DB::writeStringBinary(std::string("u1"), wb);
        DB::writeStringBinary(std::string("u2"), wb);

        wb.finalize();
    }

    StoragePolicyDescriptor deserialized;
    DB::ReadBufferFromString rb(buf);
    /// The local schema is v2, but the wire payload is v1 — the deserialize must honor
    /// the wire version, not the local one.
    deserialized.deserialize(rb, StoragePolicyDescriptor::schema_version);

    EXPECT_EQ(deserialized.data_version, kDataVersion);
    EXPECT_EQ(deserialized.name, "legacy_policy");
    EXPECT_DOUBLE_EQ(deserialized.move_factor, 0.42);

    ASSERT_EQ(deserialized.volumes.size(), 1u);
    const auto & v = deserialized.volumes[0];
    EXPECT_EQ(v.name, "legacy_vol");
    EXPECT_EQ(v.type, VolumeDescriptor::VolumeType::JBOD);
    EXPECT_EQ(v.max_data_part_size, 4096u);
    EXPECT_DOUBLE_EQ(v.max_data_part_size_ratio, 0.5);
    EXPECT_TRUE(v.perform_ttl_move_on_insert);
    EXPECT_EQ(v.load_balancing, VolumeDescriptor::VolumeLoadBalancing::RoundRobin);
    EXPECT_EQ(v.volume_priority, 7u);
    EXPECT_EQ(v.disk_names, std::vector<std::string>({"d_a", "d_b"}));

    /// v2 fields stay at struct defaults when reading v1.
    EXPECT_FALSE(v.prefer_not_to_merge);
    EXPECT_EQ(v.least_used_ttl_ms, 60'000u);

    /// And the policy-level tail after volumes was read correctly — proves the volume serde
    /// did not over-read past the v1 volume into these bytes.
    EXPECT_EQ(deserialized.create_timestamp_ms, 111);
    EXPECT_EQ(deserialized.last_modify_timestamp_ms, 222);
    EXPECT_EQ(deserialized.created_by, "u1");
    EXPECT_EQ(deserialized.last_modified_by, "u2");
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
