#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

#include <gtest/gtest.h>

TEST(UserDefinedFunctionDescriptor, ExecutableSerde)
{
    auto udf_payload = std::make_shared<cluster::protocol::ExecutableUserDefinedFunctionPayload>(
        "command",
        std::vector<std::string>{"arg1", "arg2", "arg3"},
        "format",
        100,
        200,
        300,
        400,
        500,
        true,
        true,
        true,
        cluster::Nulls::NullVersion);

    cluster::protocol::UserDefinedFunctionDescriptor desc(
        std::vector<cluster::protocol::UserDefinedFunctionDescriptor::Argument>{
            {"arg1", "type1"},
            {"arg2", "type2"},
            {"arg3", "type3"},
        },
        "test",
        "return_type",
        udf_payload,
        0,
        0,
        "created_by",
        "last_modified_by",
        cluster::Nulls::NullVersion);

    auto s = desc.string();

    EXPECT_EQ(
        s,
        "version=0, type=Executable, arguments=[(name=arg1, type=type1), (name=arg2, type=type2), (name=arg3, type=type3)], name=test, "
        "return_type=return_type, payload=(version=0, command=command, command_arguments=[arg1, arg2, arg3], "
        "format=format, command_termination_timeout_seconds=100, command_read_timeout_milliseconds=200, "
        "command_write_timeout_milliseconds=300, max_command_execution_time_seconds=400, pool_size=500, is_executable_pool=true, "
        "send_chunk_header=true, execute_direct=true)");

    auto data = cluster::serialize<std::string>(desc, cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor der_desc;
    cluster::deserialize(der_desc, data, cluster::Nulls::NullVersion);

    EXPECT_EQ(desc, der_desc);
}

TEST(UserDefinedFunctionDescriptor, RemoteSerde)
{
    auto udf_payload = std::make_shared<cluster::protocol::RemoteUserDefinedFunctionPayload>(
        100,
        10,
        "http://timeplus.com",
        cluster::protocol::RemoteUserDefinedFunctionPayload::AuthMethod::AuthHeader,
        "user",
        "password",
        /*data_version_=*/1u);

    cluster::protocol::UserDefinedFunctionDescriptor desc(
        std::vector<cluster::protocol::UserDefinedFunctionDescriptor::Argument>{
            {"arg1", "type1"},
            {"arg2", "type2"},
            {"arg3", "type3"},
        },
        "test",
        "return_type",
        udf_payload,
        0,
        0,
        "created_by",
        "last_modified_by",
        /*data_version_=*/2u);

    auto s = desc.string();

    EXPECT_EQ(
        s,
        "version=2, type=Remote, arguments=[(name=arg1, type=type1), (name=arg2, type=type2), (name=arg3, type=type3)], name=test, "
        "return_type=return_type, payload=(version=1, command_read_timeout_milliseconds=100, "
        "command_execution_timeout_milliseconds=10, url='http://timeplus.com', auth_method=AuthHeader, auth_context=(key_name=user, "
        "key_value=password))");

    auto data = cluster::serialize<std::string>(desc, cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor der_desc;
    cluster::deserialize(der_desc, data, cluster::Nulls::NullVersion);

    EXPECT_EQ(desc, der_desc);
}

TEST(UserDefinedFunctionDescriptor, JavaScriptSerde)
{
    auto udf_payload
        = std::make_shared<cluster::protocol::JavaScriptUserDefinedFunctionPayload>(true, "source code", cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor desc(
        std::vector<cluster::protocol::UserDefinedFunctionDescriptor::Argument>{
            {"arg1", "type1"},
            {"arg2", "type2"},
            {"arg3", "type3"},
        },
        "test",
        "return_type",
        udf_payload,
        0,
        0,
        "created_by",
        "last_modified_by",
        cluster::Nulls::NullVersion);

    auto s = desc.string();

    EXPECT_EQ(
        s,
        "version=0, type=Javascript, arguments=[(name=arg1, type=type1), (name=arg2, type=type2), (name=arg3, type=type3)], name=test, "
        "return_type=return_type, payload=(version=0, is_aggregation=true, source=`source code`)");

    auto data = cluster::serialize<std::string>(desc, cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor der_desc;
    cluster::deserialize(der_desc, data, cluster::Nulls::NullVersion);

    EXPECT_EQ(desc, der_desc);
}

TEST(UserDefinedFunctionDescriptor, SQLSerde)
{
    auto udf_payload = std::make_shared<cluster::protocol::SQLUserDefinedFunctionPayload>("source code", cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor desc(
        std::vector<cluster::protocol::UserDefinedFunctionDescriptor::Argument>{
            {"arg1", "type1"},
            {"arg2", "type2"},
            {"arg3", "type3"},
        },
        "test",
        "return_type",
        udf_payload,
        0,
        0,
        "created_by",
        "last_modified_by",
        cluster::Nulls::NullVersion);

    auto s = desc.string();

    EXPECT_EQ(
        s,
        "version=0, type=SQL, arguments=[(name=arg1, type=type1), (name=arg2, type=type2), (name=arg3, type=type3)], name=test, "
        "return_type=return_type, payload=(version=0, query=`source code`)");

    auto data = cluster::serialize<std::string>(desc, cluster::Nulls::NullVersion);
    cluster::protocol::UserDefinedFunctionDescriptor der_desc;
    cluster::deserialize(der_desc, data, cluster::Nulls::NullVersion);

    EXPECT_EQ(desc, der_desc);
}
