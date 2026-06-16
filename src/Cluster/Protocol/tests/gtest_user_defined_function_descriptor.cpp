#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

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
        1720000000000, // create_timestamp_ms
        1720000005000, // last_modify_timestamp_ms
        "test_user",
        "admin_user",
        5 // data_version
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
    ExecutableUserDefinedFunctionPayload payload1("cmd", {"arg1", "arg2"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    ExecutableUserDefinedFunctionPayload payload2("cmd", {"arg1", "arg2"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    EXPECT_TRUE(payload1.operator==(payload2));

    payload2.command = "cmd2";
    EXPECT_FALSE(payload1.operator==(payload2));
}

TEST(UserDefinedFunctionDescriptor, StringAndType)
{
    ExecutableUserDefinedFunctionPayload payload("cmd", {"arg1"}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

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
    ExecutableUserDefinedFunctionPayload payload("cmd", {}, "JSON", 10, 1000, 2000, 30, 16, true, true, true, 1);

    auto ast_str = payload.createASTString("my_udf", "(arg1 UInt32)", "UInt32");
    ASSERT_FALSE(ast_str.empty());
    ASSERT_NE(ast_str.find("my_udf"), std::string::npos);
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadInitSettingsRoundTrip)
{
    PythonUserDefinedFunctionPayload payload;
    payload.data_version = 3;
    payload.is_aggregation = false;
    payload.using_numpy = true;
    payload.source = "def f(x):\n    return x";
    payload.init_function_name = "_tp_init";
    payload.named_collection = "creds";

    DB::WriteBufferFromOwnString wb;
    payload.serialize(wb, 0);

    DB::ReadBufferFromString rb(wb.str());
    PythonUserDefinedFunctionPayload restored;
    restored.deserialize(rb, 0);

    EXPECT_TRUE(payload.operator==(restored));
    EXPECT_EQ(restored.init_function_name, "_tp_init");
    EXPECT_EQ(restored.named_collection, "creds");
    EXPECT_EQ(restored.init_function_parameters, "");
    EXPECT_TRUE(restored.using_numpy);
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadV1Compat)
{
    /// Hand-crafted v1 byte stream: v1 wrote only
    /// (schema_data_version, is_aggregation, using_numpy, source).
    DB::WriteBufferFromOwnString wb;
    uint64_t schema_data_version = (static_cast<uint64_t>(1) << 32) + 7;
    DB::writeVarUInt(schema_data_version, wb);
    DB::writeBinary(true, wb); /// is_aggregation
    DB::writeBinary(false, wb); /// using_numpy
    DB::writeStringBinary(std::string("def g(x):\n    return x"), wb);

    DB::ReadBufferFromString rb(wb.str());
    PythonUserDefinedFunctionPayload restored;
    restored.deserialize(rb, 0);

    EXPECT_EQ(restored.data_version, 7u);
    EXPECT_TRUE(restored.is_aggregation);
    EXPECT_EQ(restored.source, "def g(x):\n    return x");
    EXPECT_EQ(restored.init_function_name, "");
    EXPECT_EQ(restored.init_function_parameters, "");
    EXPECT_EQ(restored.named_collection, "");
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadV1WireWhenNoInitHook)
{
    /// A payload without an init hook MUST serialize to the exact v1 layout so a
    /// pre-v2 peer reads it byte-for-byte. The payload is written inline (no length
    /// prefix) and is followed on the wire by request fields (exists_op/initiator/
    /// timeout_ms); if we emitted the v2 trailing strings here, a pre-v2 reader would
    /// leave them unconsumed and misalign those following fields.
    PythonUserDefinedFunctionPayload payload;
    payload.data_version = 7;
    payload.is_aggregation = true;
    payload.using_numpy = false;
    payload.source = "def g(x):\n    return x";
    /// no init_function_name / init_function_parameters / named_collection

    DB::WriteBufferFromOwnString wb;
    payload.serialize(wb, 0);

    DB::ReadBufferFromString rb(wb.str());
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, rb);
    EXPECT_EQ(static_cast<uint32_t>(schema_data_version >> 32), 1u) << "no init hook must encode schema_version 1 on the wire";

    bool is_aggregation = false;
    bool using_numpy = false;
    std::string source;
    DB::readBinary(is_aggregation, rb);
    DB::readBinary(using_numpy, rb);
    DB::readStringBinary(source, rb);
    EXPECT_EQ(source, "def g(x):\n    return x");
    /// Crux: after the v1 fields the payload region is fully consumed — nothing for a
    /// pre-v2 reader to misinterpret as the request fields that follow the descriptor.
    EXPECT_TRUE(rb.eof()) << "v1-compatible payload must not write trailing v2 bytes";
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadV2WireWhenInitHookSet)
{
    /// With an init hook configured we intentionally emit the v2 layout; round-trip
    /// must preserve every field and the wire must carry schema_version 2.
    PythonUserDefinedFunctionPayload payload;
    payload.data_version = 3;
    payload.source = "def f(x):\n    return x";
    payload.init_function_name = "_tp_init";

    DB::WriteBufferFromOwnString wb;
    payload.serialize(wb, 0);

    DB::ReadBufferFromString peek(wb.str());
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, peek);
    EXPECT_EQ(static_cast<uint32_t>(schema_data_version >> 32), 2u) << "configured init hook must encode schema_version 2";

    DB::ReadBufferFromString rb(wb.str());
    PythonUserDefinedFunctionPayload restored;
    restored.deserialize(rb, 0);
    EXPECT_TRUE(payload.operator==(restored));
    EXPECT_EQ(restored.init_function_name, "_tp_init");
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadEquality)
{
    PythonUserDefinedFunctionPayload a;
    a.source = "src";
    PythonUserDefinedFunctionPayload b = a;
    EXPECT_TRUE(a.operator==(b));

    b.using_numpy = !a.using_numpy;
    EXPECT_FALSE(a.operator==(b));

    b = a;
    b.init_function_name = "_tp_init";
    EXPECT_FALSE(a.operator==(b));

    b = a;
    b.init_function_parameters = "{\"k\":\"v\"}";
    EXPECT_FALSE(a.operator==(b));

    b = a;
    b.named_collection = "creds";
    EXPECT_FALSE(a.operator==(b));
}

TEST(UserDefinedFunctionDescriptor, PythonPayloadCreateASTStringWithSettings)
{
    PythonUserDefinedFunctionPayload payload;
    payload.source = "def f(x):\n    return x";
    payload.init_function_name = "_tp_init";
    payload.init_function_parameters = "{\"k\":\"it's\"}";

    auto ast = payload.createASTString("f", "x string", "string");
    EXPECT_NE(ast.find("SETTINGS init_function_name = '_tp_init'"), std::string::npos);
    /// The single quote inside the JSON parameters must be SQL-escaped.
    EXPECT_NE(ast.find("init_function_parameters = '{\"k\":\"it\\'s\"}'"), std::string::npos);

    PythonUserDefinedFunctionPayload with_collection;
    with_collection.source = "def f(x):\n    return x";
    with_collection.init_function_name = "_tp_init";
    with_collection.named_collection = "creds";
    auto ast2 = with_collection.createASTString("f", "x string", "string");
    EXPECT_NE(ast2.find("named_collection = 'creds'"), std::string::npos);

    PythonUserDefinedFunctionPayload no_settings;
    no_settings.source = "def f(x):\n    return x";
    EXPECT_EQ(no_settings.createASTString("f", "x string", "string").find("SETTINGS"), std::string::npos);
}
