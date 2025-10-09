#include <gtest/gtest.h>

#include <Cluster/Protocol/RequestHeaderData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

using namespace cluster::protocol;

TEST(RequestHeaderDataTest, SerializeDeserialize)
{
    OpCode opcode = OpCode::RenameStream;
    uint16_t version = 1;
    uint64_t correlation_id = 0xABCDEF123456;
    int64_t timestamp_ms = 1650009876543;

    RequestHeaderData original(opcode, version, correlation_id, timestamp_ms);

    DB::WriteBufferFromOwnString write_buf;
    original.serialize(write_buf, /*version*/ 0);

    RequestHeaderData deserialized;
    DB::ReadBufferFromString read_buf(write_buf.str());
    deserialized.deserialize(read_buf, /*version*/ 0);

    EXPECT_EQ(deserialized.requestOpCode(), opcode);
    EXPECT_EQ(deserialized.requestVersion(), version);
    EXPECT_EQ(deserialized.correlationID(), correlation_id);
    EXPECT_EQ(deserialized.elapsedMillisecondsSinceCreation() <= DB::UTCMilliseconds::now() - timestamp_ms + 10, true); // 容忍部分误差

    std::string expected_str = fmt::format(
        "request_opcode={} request_version={} correlation_id=0x{:x} timestamp_ms={}",
        magic_enum::enum_name(opcode),
        version,
        correlation_id,
        timestamp_ms);

    EXPECT_EQ(deserialized.doString(), expected_str);
}
