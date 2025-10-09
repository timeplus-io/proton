#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace cluster::protocol;

TEST(UserDefinedFunctionDescriptor, SerializeDeserialize)
{
    UserDefinedFunctionDescriptor::Argument arg1{"arg1", "UInt64"};
    UserDefinedFunctionDescriptor::Argument arg2{"arg2", "String"};
    std::vector<UserDefinedFunctionDescriptor::Argument> args = {arg1, arg2};

    auto payload = std::make_shared<SQLUserDefinedFunctionPayload>("SELECT arg1 + length(arg2)", 5);

    UserDefinedFunctionDescriptor original(
        args,
        "testFunction",
        "UInt64",
        payload,
        1720000000000,  // create_timestamp_ms
        1720000005000,  // last_modify_timestamp_ms
        "test_user",
        "admin_user",
        5  // data_version
    );

    // Serialize
    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, UserDefinedFunctionDescriptor::schema_version);

    // Deserialize
    DB::ReadBufferFromString rb(wb.str());
    UserDefinedFunctionDescriptor restored;
    restored.deserialize(rb, UserDefinedFunctionDescriptor::schema_version);

    // Assert Equal
    EXPECT_EQ(original, restored);
}

TEST(UserDefinedFunctionDescriptor, EqualsOperator)
{
    ExecutableUserDefinedFunctionPayload payload1(
        "cmd", {"arg1", "arg2"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    ExecutableUserDefinedFunctionPayload payload2(
        "cmd", {"arg1", "arg2"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    EXPECT_TRUE(payload1.operator==(payload2));

    payload2.command = "cmd2";
    EXPECT_FALSE(payload1.operator==(payload2));
}

TEST(UserDefinedFunctionDescriptor, StringAndType)
{
    ExecutableUserDefinedFunctionPayload payload(
        "cmd", {"arg1"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    auto str = payload.string();
    ASSERT_FALSE(str.empty());
    ASSERT_EQ(payload.type(), UDFType::Executable);
}

TEST(UserDefinedFunctionDescriptor, IsAggregationAndSourceView)
{
    ExecutableUserDefinedFunctionPayload payload;
    ASSERT_FALSE(payload.isAggregation());
    ASSERT_TRUE(payload.sourceView().empty());
}

TEST(UserDefinedFunctionDescriptor, CreateASTString)
{
    ExecutableUserDefinedFunctionPayload payload(
        "cmd", {}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    auto ast_str = payload.createASTString("my_udf", "(arg1 UInt32)", "UInt32");
    ASSERT_FALSE(ast_str.empty());
    ASSERT_NE(ast_str.find("my_udf"), std::string::npos);
}