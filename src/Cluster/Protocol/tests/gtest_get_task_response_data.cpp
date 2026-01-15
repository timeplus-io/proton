#include <Cluster/Protocol/GetTaskResponseData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace cluster;
using namespace cluster::protocol;

TEST(GetTaskResponseDataTest, SerializeDeserializeNoError)
{
    using namespace cluster::protocol;

    TaskDescriptor task_desc;

    task_desc.id = DB::UUIDHelpers::generateV4();
    task_desc.ns = "default_namespace";
    task_desc.name = "task_123";
    task_desc.sql = "SELECT * FROM test_table";

    task_desc.create_timestamp_ms = 1723363200000;
    task_desc.last_modify_timestamp_ms = 1723363200000;
    task_desc.created_by = "user_a";
    task_desc.last_modified_by = "user_b";

    task_desc.cron = "0 0 * * *";
    task_desc.interval = 10;
    task_desc.interval_unit.kind = DB::IntervalKind::Kind::Second;
    task_desc.timeout = 300;
    task_desc.timeout_unit = DB::IntervalKind(DB::IntervalKind::Kind::Second);

    task_desc.checkpoint_columns = {"col1", "col2"};
    task_desc.checkpoint_init_values = "init_values";

    task_desc.target_database_name = "db";
    task_desc.target_table_name = "table";
    task_desc.status = TaskStatus::Disabled;
    task_desc.data_version = 1;

    DB::WriteBufferFromOwnString wb;
    task_desc.serialize(wb, 0);

    TaskDescriptor new_task_desc;
    DB::ReadBufferFromString rb(wb.str());
    new_task_desc.deserialize(rb, 0);

    EXPECT_EQ(task_desc.name, new_task_desc.name);
    EXPECT_EQ(task_desc.ns, new_task_desc.ns);
    EXPECT_EQ(task_desc.sql, new_task_desc.sql);
    EXPECT_EQ(task_desc.status, new_task_desc.status);
}


TEST(GetTaskResponseDataTest, SerializeDeserializeWithError)
{
    Error err(1001, "some error");
    GetTaskResponseData original(std::move(err));

    DB::WriteBufferFromOwnString wb;
    original.serialize(wb, 1);
    std::string serialized = wb.str();

    GetTaskResponseData restored;
    DB::ReadBufferFromString rb(serialized);
    restored.deserialize(rb, 1);

    EXPECT_TRUE(restored.hasError());
    EXPECT_EQ(restored.errorString(), "error_code=1001 error_message='some error'");
}
