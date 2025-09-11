#include <Cluster/Protocol/AlterTaskRequestData.h>
#include <Cluster/Protocol/AlterTaskResponseData.h>
#include <Cluster/Protocol/CreateTaskRequestData.h>
#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/AlterTaskRequest.h>
#include <Cluster/Requests/AlterTaskResponse.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>


using namespace cluster;
using namespace cluster::protocol;

TEST(Task, TaskDescriptor)
{
    TaskDescriptor task;
    task.data_version = 3;

    task.id = DB::UUIDHelpers::generateV4();
    task.ns = "test_database";
    task.name = "test_task";

    task.create_timestamp_ms = 1234;
    task.last_modify_timestamp_ms = 5678;

    task.created_by = "creator";
    task.last_modified_by = "modifier";

    task.cron = "* 0/5 * * *";
    task.interval = 10;
    task.interval_unit = DB::IntervalKind::Kind::Day;

    task.timeout = 42;
    task.timeout_unit = DB::IntervalKind::Kind::Millisecond;

    task.checkpoint_columns = {"col1", "col2", "col3"};
    task.checkpoint_init_values = R"({"col1": "val1", "col2": "val2", "col3": "val3"}")";

    task.target_database_name = "into_database";
    task.target_table_name = "into_table";

    task.sql = "SELECT ${col1} FROM ${col2}";

    task.status = TaskStatus::Disabled;

    std::string buf_str;
    {
        DB::WriteBufferFromString wb{buf_str};
        task.serialize(wb, 1);
    }

    ASSERT_LE(buf_str.size(), task.approximateSerializedSize());

    DB::ReadBufferFromMemory rb{buf_str};
    TaskDescriptor deserialized;
    deserialized.deserialize(rb, 1);

    ASSERT_EQ(deserialized.data_version, 3);
    ASSERT_EQ(deserialized.ns, "test_database");
    ASSERT_EQ(deserialized.name, "test_task");
    ASSERT_EQ(deserialized.create_timestamp_ms, 1234);
    ASSERT_EQ(deserialized.last_modify_timestamp_ms, 5678);
    ASSERT_EQ(deserialized.created_by, "creator");
    ASSERT_EQ(deserialized.last_modified_by, "modifier");
    ASSERT_EQ(deserialized.cron, "* 0/5 * * *");
    ASSERT_EQ(deserialized.interval, 10);
    ASSERT_EQ(deserialized.interval_unit, DB::IntervalKind::Kind::Day);
    ASSERT_EQ(deserialized.timeout, 42);
    ASSERT_EQ(deserialized.timeout_unit, DB::IntervalKind::Kind::Millisecond);
    ASSERT_EQ(deserialized.checkpoint_columns.size(), 3);
    ASSERT_EQ(deserialized.checkpoint_columns[0], "col1");
    ASSERT_EQ(deserialized.checkpoint_columns[1], "col2");
    ASSERT_EQ(deserialized.checkpoint_columns[2], "col3");
    ASSERT_EQ(deserialized.checkpoint_init_values, R"({"col1": "val1", "col2": "val2", "col3": "val3"}")");
    ASSERT_EQ(deserialized.target_database_name, "into_database");
    ASSERT_EQ(deserialized.target_table_name, "into_table");
}

TEST(Task, CreateTaskRequestData)
{
    CreateTaskRequestData req_data;
    req_data.initiator = 666;
    req_data.timeout_ms = 1234;
    req_data.exists_op = ExistsOperation::Replace;
    req_data.requested_by = "a_task_creator";
    req_data.requested_ts = 12345678;

    ASSERT_EQ(req_data.opCode(), OpCode::CreateTask);

    auto req_data_str = req_data.doString();
    const std::string expected_str = "initiator=0x29a timeout_ms=1234 exists_op=Replace requested_by=a_task_creator requested_ts=12345678";
    ASSERT_EQ(req_data_str.substr(req_data_str.find("initiator=")), expected_str);

    std::string buf_str;
    {
        DB::WriteBufferFromString wb{buf_str};
        req_data.serialize(wb, 1);
    }
    ASSERT_LT(buf_str.size(), req_data.approximateSerializedSize());

    CreateTaskRequestData deserialized_data;
    deserialized_data.deserialize(buf_str, 1);

    ASSERT_EQ(deserialized_data.initiator, 666);
    ASSERT_EQ(deserialized_data.timeout_ms, 1234);
    ASSERT_EQ(deserialized_data.exists_op, ExistsOperation::Replace);
    ASSERT_EQ(deserialized_data.requested_by, "a_task_creator");
    ASSERT_EQ(deserialized_data.requested_ts, 12345678);
}

TEST(Task, AlterTaskRequestData)
{
    std::string ns = "task_namespace";
    std::string name = "task_name_1234";
    DB::UUID uuid = DB::UUIDHelpers::generateV4();
    uint32_t data_version = 42;
    AlterTaskRequestData::AlterType type = AlterTaskRequestData::AlterType::STATUS;
    TaskStatus status = TaskStatus::Disabled;
    std::string requested_by = "tester";
    NodeID initiator = 12345;
    int64_t timeout_ms = 3000;

    AlterTaskRequestData data(ns, name, uuid, data_version, type, status, requested_by, initiator, timeout_ms);

    /// Constructor sets fields correctly.
    EXPECT_EQ(data.ns, ns);
    EXPECT_EQ(data.name, name);
    EXPECT_EQ(data.id, uuid);
    EXPECT_EQ(data.data_version, data_version);
    EXPECT_EQ(data.type, type);
    EXPECT_EQ(data.status, status);
    EXPECT_EQ(data.requested_by, requested_by);
    EXPECT_GT(data.requested_ts, 0);
    EXPECT_EQ(data.initiator, initiator);
    EXPECT_EQ(data.timeout_ms, timeout_ms);

    /// OpCode returns correct value.
    EXPECT_EQ(data.opCode(), OpCode::AlterTask);

    /// Serialize and deserialize roundtrip.
    DB::WriteBufferFromOwnString wb;
    data.serialize(wb, 1);

    AlterTaskRequestData deserialized;
    deserialized.deserialize(wb.str(), 1);

    EXPECT_EQ(deserialized.ns, data.ns);
    EXPECT_EQ(deserialized.name, data.name);
    EXPECT_EQ(deserialized.id, data.id);
    EXPECT_EQ(deserialized.data_version, data.data_version);
    EXPECT_EQ(deserialized.type, data.type);
    EXPECT_EQ(deserialized.status, data.status);
    EXPECT_EQ(deserialized.requested_by, data.requested_by);
    EXPECT_EQ(deserialized.requested_ts, data.requested_ts);
    EXPECT_EQ(deserialized.initiator, data.initiator);
    EXPECT_EQ(deserialized.timeout_ms, data.timeout_ms);
}

TEST(Task, AlterTaskResponseDataNoError)
{
    AlterTaskResponseData original;
    original.replica_leader = 123;
    original.sn = 456;

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);

    AlterTaskResponseData deserialized;
    deserialized.deserialize(wb.str(), 1);

    EXPECT_FALSE(deserialized.hasError());
    EXPECT_EQ(deserialized.replica_leader, 123);
    EXPECT_EQ(deserialized.sn, 456);
}

TEST(Task, AlterTaskResponseDataWithError)
{
    AlterTaskResponseData original(1001, "Internal Error");

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);

    AlterTaskResponseData deserialized;
    deserialized.deserialize(wb.str(), 1);

    EXPECT_TRUE(deserialized.hasError());
    EXPECT_EQ(deserialized.error().error_code, 1001);
    EXPECT_EQ(deserialized.error().error_message, "Internal Error");

    // Ensure optional fields are left untouched in error case
    EXPECT_EQ(deserialized.replica_leader, Nulls::NullNodeID);
    EXPECT_EQ(deserialized.sn, 0);
}

TEST(Task, AlterTaskRequestUpdateStatus)
{
    const std::string ns = "ns_update_status";
    const std::string name = "task_name_1417";
    const auto uuid = DB::UUIDHelpers::generateV4();
    uint32_t data_version = 5;
    TaskStatus status = TaskStatus::Disabled;
    const std::string created_by = "tester";
    NodeID initiator = 0x42;
    int64_t timeout_ms = 5000;
    uint16_t request_version = 1;

    const auto req
        = std::make_shared<AlterTaskRequest>(ns, name, uuid, data_version, status, created_by, initiator, timeout_ms, request_version);

    ASSERT_NE(req, nullptr);

    const AlterTaskRequestData & data = req->data();

    EXPECT_EQ(data.ns, ns);
    EXPECT_EQ(data.name, name);
    EXPECT_EQ(data.id, uuid);
    EXPECT_EQ(data.data_version, data_version);
    EXPECT_EQ(data.status, status);
    EXPECT_EQ(data.type, AlterTaskRequestData::AlterType::STATUS);
    EXPECT_EQ(data.initiator, initiator);
    EXPECT_EQ(data.timeout_ms, timeout_ms);
    EXPECT_EQ(data.requested_by, created_by);
    EXPECT_GT(data.requested_ts, 0); // should be initialized to now
    EXPECT_EQ(req->version(), request_version);
}

TEST(Task, AlterTaskResponseWithSuccess)
{
    NodeID leader = 0x10;
    int64_t sn = 999;
    AlterTaskResponse resp(leader, sn, 1);

    EXPECT_FALSE(resp.hasError());
    const auto & data = resp.data();
    EXPECT_EQ(data.replica_leader, leader);
    EXPECT_EQ(data.sn, sn);
}

TEST(Task, AlterTaskResponseWithError)
{
    {
        Error err(5678, "Custom error");
        AlterTaskResponse resp(std::move(err), 3);

        EXPECT_TRUE(resp.hasError());
        EXPECT_EQ(resp.error().error_code, 5678);
        EXPECT_EQ(resp.error().error_message, "Custom error");
        EXPECT_EQ(resp.version(), 3);
    }

    {
        std::string msg = "Some failure";
        AlterTaskResponse resp(1234, std::move(msg), 2);

        EXPECT_TRUE(resp.hasError());
        EXPECT_EQ(resp.error().error_code, 1234);
        EXPECT_EQ(resp.error().error_message, "Some failure");
        EXPECT_EQ(resp.version(), 2);
    }
}
