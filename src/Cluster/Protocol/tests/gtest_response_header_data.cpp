#include <gtest/gtest.h>

#include <Cluster/Protocol/ResponseHeaderData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

using namespace cluster::protocol;

TEST(ResponseHeaderDataTest, SerializeDeserialize)
{
    OpCode opcode = OpCode::CreateStream;
    uint16_t version = 1;
    uint64_t correlation_id = 0x123456789ABCDEF;

    ResponseHeaderData original(opcode, version, correlation_id);

    DB::WriteBufferFromOwnString write_buf;
    original.serialize(write_buf, 0);

    ResponseHeaderData deserialized;
    DB::ReadBufferFromString read_buf(write_buf.str());
    deserialized.deserialize(read_buf, 0);

    EXPECT_EQ(deserialized.responseOpCode(), opcode);
    EXPECT_EQ(deserialized.responseVersion(), version);
    EXPECT_EQ(deserialized.correlationID(), correlation_id);
}
