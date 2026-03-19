#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterCreateTaskQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/CreateTaskRequest.h>
#include <Cluster/Requests/CreateTaskResponse.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Cluster/Requests/GetTaskResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateTaskQuery.h>
#include <Task/TaskExecutionResult.h>
#include <Task/Utils.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CREATE_TASK;
extern const int RESOURCE_ALREADY_EXISTS;
extern const int SYNTAX_ERROR;
extern const int UNKNOWN_DATABASE;
extern const int UNSUPPORTED;
}

BlockIO InterpreterCreateTaskQuery::execute()
{
    auto & create_task_query = query_ptr->as<ASTCreateTaskQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_TASK);

    auto replace_if_exists = create_task_query.or_replace;
    if (replace_if_exists)
        access_rights_elements.emplace_back(AccessType::DROP_TASK);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    auto exists_op = cluster::protocol::ExistsOperation::Throw;
    if (create_task_query.if_not_exists)
        exists_op = cluster::protocol::ExistsOperation::Ignore;
    else if (create_task_query.or_replace)
        exists_op = cluster::protocol::ExistsOperation::Replace;

    String database;
    if (create_task_query.database)
    {
        database = create_task_query.getDatabase();
        if (!DatabaseCatalog::instance().isDatabaseExist(database))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database);
    }
    else
    {
        database = current_context->getCurrentDatabase();
    }

    /// Validate the task query and target stream before creating the task
    Task::validateCreateTaskQuery(&create_task_query, current_context);

    auto task_name = create_task_query.getName();
    UInt32 version{1};
    DB::UUID id;
    auto req = std::make_shared<cluster::GetTaskRequest>(
        database,
        task_name,
        1, // versions_requested
        0, // initiator (single-instance: always 0)
        false, // consistent_read
        30000, // timeout_ms
        1 // request_version
    );
    auto resp = metastore.getTask(req);
    if (const auto err = resp->error(); err.hasError())
    {
        if (!err.isNotFound())
            throw Exception(
                ErrorCodes::CANNOT_CREATE_TASK,
                "Failed to check if task {} aleady exists in {}: error_code={} error_message={}",
                task_name,
                database,
                err.error_code,
                err.error_message);

        id = DB::UUIDHelpers::generateV4();
    }
    else
    {
        if (exists_op == cluster::protocol::ExistsOperation::Ignore)
            return {};
        else if (exists_op == cluster::protocol::ExistsOperation::Throw)
            throw Exception(ErrorCodes::RESOURCE_ALREADY_EXISTS, "Task {} already exists in {}", task_name, database);

        version = resp->data().descs[0]->data_version;
        id = resp->data().descs[0]->id;
    }

    cluster::protocol::TaskDescriptor desc{id, database, task_name, create_task_query.sql, version};
    desc.cron = create_task_query.cron;

    desc.interval = create_task_query.interval;
    desc.interval_unit = create_task_query.interval_unit;

    desc.timeout = create_task_query.timeout;
    desc.timeout_unit = create_task_query.timeout_unit;

    desc.checkpoint_columns = create_task_query.checkpoint_names;
    desc.checkpoint_init_values = create_task_query.checkpoint;

    if (create_task_query.to_table_id)
    {
        if (!create_task_query.to_table_id.database_name.empty())
        {
            const auto & to_database_name = create_task_query.to_table_id.database_name;
            if (!DatabaseCatalog::instance().isDatabaseExist(to_database_name))
                throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", to_database_name);

            desc.target_database_name = to_database_name;
        }
        else
        {
            desc.target_database_name = current_context->getCurrentDatabase();
        }

        desc.target_table_name = create_task_query.to_table_id.table_name;
    }

    auto create_task_req = std::make_shared<cluster::CreateTaskRequest>(
        std::move(desc),
        exists_op,
        /*created_by=*/current_context->getUserName(),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/1);

    auto create_task_resp = metastore.createTask(std::move(create_task_req));
    if (create_task_resp->hasError())
    {
        const auto & err = create_task_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_TASK,
            "Failed to create task {} in {}: error_code={} error_message={}",
            task_name,
            database,
            err.error_code,
            err.error_message);
    }

    return {};
}
}
