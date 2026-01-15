#include <gtest/gtest.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/CreateTaskRequestData.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace cluster;
using namespace cluster::protocol;

TEST(CreateTaskRequestDataTest, SerializeDeserialize)
{
    TaskDescriptor task_desc;
    task_desc.data_version = 1;
    task_desc.id = DB::UUIDHelpers::generateV4();
    task_desc.ns = "default";
    task_desc.name = "task_A";
    task_desc.sql = "SELECT * FROM test";
    task_desc.create_timestamp_ms = 1111;
    task_desc.last_modify_timestamp_ms = 2222;
    task_desc.created_by = "creator";
    task_desc.last_modified_by = "modifier";
    task_desc.cron = "0 * * * *";
    task_desc.interval = 10;
    task_desc.interval_unit.kind = DB::IntervalKind::Kind::Second;
    task_desc.timeout = 5000;
    task_desc.timeout_unit.kind = DB::IntervalKind::Kind::Millisecond;
    task_desc.checkpoint_columns = {"col1", "col2"};
    task_desc.checkpoint_init_values = "init_val";
    task_desc.target_database_name = "db";
    task_desc.target_table_name = "table";
    task_desc.status = TaskStatus::Disabled;

    NodeID initiator = 12345;
    int64_t timeout_ms = 60000;
    ExistsOperation exists_op = ExistsOperation::Throw;
    std::string requested_by = "tester";

    CreateTaskRequestData data(std::move(task_desc), exists_op, requested_by, initiator, timeout_ms);

    DB::WriteBufferFromOwnString wb;
    data.serialize(wb, /*version*/ 1);
    std::string serialized = wb.str();

    DB::ReadBufferFromString rb(serialized);
    CreateTaskRequestData deserialized;
    deserialized.deserialize(rb, /*version*/ 1);

    EXPECT_EQ(deserialized.desc.data_version, 1u);
    EXPECT_EQ(deserialized.desc.ns, "default");
    EXPECT_EQ(deserialized.desc.name, "task_A");
    EXPECT_EQ(deserialized.desc.sql, "SELECT * FROM test");
    EXPECT_EQ(deserialized.desc.created_by, "creator");
    EXPECT_EQ(deserialized.desc.last_modified_by, "modifier");
    EXPECT_EQ(deserialized.initiator, initiator);
    EXPECT_EQ(deserialized.timeout_ms, timeout_ms);
    EXPECT_EQ(deserialized.exists_op, exists_op);
    EXPECT_EQ(deserialized.requested_by, requested_by);
    EXPECT_GT(deserialized.requested_ts, 0);
}
