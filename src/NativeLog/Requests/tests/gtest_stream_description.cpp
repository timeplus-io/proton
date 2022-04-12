#include <Core/UUID.h>
#include <NativeLog/Base/Utils.h>
#include <NativeLog/Requests/StreamDescription.h>
#include <base/ClockUtils.h>

#include <gtest/gtest.h>

TEST(StreamDescription, Serde)
{
    nlog::StreamDescription desc;
    desc.version = 11;
    desc.id = DB::UUIDHelpers::generateV4();
    desc.ns = "test";
    desc.stream = "stream";
    desc.shards = 3;
    desc.replicas = 2;
    desc.compacted = true;
    desc.codec = DB::CompressionMethodByte::LZ4;
    desc.flush_ms = 12345;
    desc.flush_messages = 67890;
    desc.retention_ms = 3456;
    desc.retention_bytes = 7890;
    desc.create_timestamp_ms = DB::UTCMilliseconds::now();
    desc.last_modify_timestamp_ms = DB::UTCMilliseconds::now();

    auto data{desc.serialize()};
    auto der_desc{desc.deserialize(data.data(), data.size())};

    EXPECT_EQ(desc, der_desc);
}
