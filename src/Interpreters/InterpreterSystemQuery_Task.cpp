#include <Interpreters/InterpreterSystemQuery.h>

#include <Access/ContextAccess.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/AlterTaskRequest.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSystemQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/TaskExecutionSource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Task/TaskExecution.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_TASK;
}

void InterpreterSystemQuery::updateTaskStatus(String database, const String & task_name, bool enable)
{
    if (database.empty())
        database = getContext()->getCurrentDatabase();

    getContext()->checkAccess(AccessType::SYSTEM_TASK, database, task_name);

    auto & meta_store = Globals::getMetaStore();
    auto req = std::make_shared<cluster::GetTaskRequest>(
        database,
        task_name,
        /*versions_requested=*/1,
        meta_store.nodeID(),
        /*consistent_read=*/false,
        /*timeout_ms=*/10000,
        /*request_version=*/1);

    auto resp = meta_store.getTask(std::move(req));
    if (resp->hasError())
        throw Exception(
            resp->error().error_code, "Failed to load task {} in database {}, error={{{}}}", task_name, database, resp->error().string());

    if (resp->data().descs.size() != 1)
        throw Exception(
            resp->error().error_code,
            "Get invalid number of TaskDescriptor: database={} task={} "
            "descriptors={}",
            database,
            task_name,
            resp->data().descs.size());

    const auto new_status = enable ? cluster::protocol::TaskStatus::Enabled : cluster::protocol::TaskStatus::Disabled;
    auto & desc = resp->data().descs[0];
    if (desc->status == new_status)
        return;

    auto alter_task_req = std::make_shared<cluster::AlterTaskRequest>(
        desc->ns,
        desc->name,
        desc->id,
        desc->data_version,
        new_status,
        getContext()->getUserName(),
        meta_store.nodeID(),
        /*timeout_ms=*/10000,
        /*request_version=*/1);

    auto alter_task_resp = meta_store.alterTask(std::move(alter_task_req));
    if (alter_task_resp->hasError())
    {
        const auto & err = alter_task_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_TASK,
            "Failed to set task status: database={} task={} "
            "new_status={} error={{{}}}",
            database,
            task_name,
            new_status,
            err.error_message);
    }
}

void InterpreterSystemQuery::executePauseTask(const ASTSystemQuery & system)
{
    updateTaskStatus(system.getDatabase(), system.getTable(), false);
}

void InterpreterSystemQuery::executeResumeTask(const ASTSystemQuery & system)
{
    updateTaskStatus(system.getDatabase(), system.getTable(), true);
}

BlockIO InterpreterSystemQuery::executeExecuteTask(const ASTSystemQuery & system)
{
    auto database = system.getDatabase();
    if (database.empty())
        database = getContext()->getCurrentDatabase();

    StorageID task_id{database, system.getTable()};
    auto source = std::make_shared<TaskExecutionSource>(std::move(task_id), getContext());

    BlockIO res;
    res.pipeline = QueryPipeline(std::move(source));
    return res;
}
}
