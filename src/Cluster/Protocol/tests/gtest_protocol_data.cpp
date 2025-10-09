#include <gtest/gtest.h>

#include <Cluster/Protocol/ProtocolData.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Cluster/Common/serde.h>
#include <sstream>

using namespace cluster::protocol;

class TestProtocolData : public ProtocolData
{
public:
    TestProtocolData() = default;
    explicit TestProtocolData(std::string msg_) : msg(std::move(msg_)) {}

    cluster::protocol::OpCode opCode() const noexcept override
    {
        return cluster::protocol::OpCode::CreateStream;  // Just an example opcode
    }

    void serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const override
    {
        DB::writeStringBinary(msg, wb);
    }

    size_t approximateSerializedSize() const noexcept override
    {
        return msg.size() + 1;
    }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override
    {
        return {1, 1};
    }

private:
    std::string msg;

    void doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/) override
    {
        DB::readStringBinary(msg, rb);
    }

    std::string doString() const override
    {
        return "msg=" + msg;
    }
};

TEST(ProtocolDataTest, SerializeDeserialize)
{
    TestProtocolData original_data("hello world");

    std::string serialized = cluster::serialize<std::string>(original_data, 1);
    EXPECT_FALSE(serialized.empty());

    TestProtocolData new_data;
    EXPECT_FALSE(new_data.isInitialized());
    new_data.deserialize(serialized, 1);
    EXPECT_TRUE(new_data.isInitialized());
}


TEST(ProtocolDataTest, ToStringOutput)
{
    TestProtocolData data("test msg");
    std::string output = data.string();

    EXPECT_NE(output.find("opcode="), std::string::npos);
    EXPECT_NE(output.find("msg=test msg"), std::string::npos);
}

TEST(ProtocolDataTest, OpCodeString)
{
    TestProtocolData data;
    std::string opcode_str = data.opCodeString();

    EXPECT_EQ(opcode_str, "CreateStream");
}
