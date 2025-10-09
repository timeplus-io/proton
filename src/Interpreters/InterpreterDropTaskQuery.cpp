#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropTaskQuery.h>

#include <Access/ContextAccess.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/DeleteTaskRequest.h>
#include <Cluster/Requests/DeleteTaskResponse.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Cluster/Requests/GetTaskResponse.h>
#include <Parsers/ASTDropTaskQuery.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_DROP_TASK;
extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterDropTaskQuery::execute()
{
    ASTDropTaskQuery & query = query_ptr->as<ASTDropTaskQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_TASK);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    String database;
    if (query.database)
    {
        database = query.getDatabase();
        if (!DatabaseCatalog::instance().isDatabaseExist(database))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database);
    }
    else
    {
        database = current_context->getCurrentDatabase();
    }

    auto id = DB::UUIDHelpers::Nil;

    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::GetTaskRequest>(
        database,
        query.getName(),
        1, // versions_requested
        0, // initiator (single-instance: always 0)
        false, // consistent_read
        30000, // timeout_ms
        1 // request_version
    );
    auto get_resp = metastore.getTask(req);
    if (const auto err = get_resp->error(); err.hasError())
    {
        if (!err.isNotFound())
            throw Exception(
                err.error_code, "Failed to check if task {} exists in database {}: error={}", query.getName(), database, err.error_message);

        if (query.if_exists)
            return {};

        throw Exception(err.error_code, "Task {} was not found in database {}", query.getName(), database);
    }
    else
    {
        chassert(get_resp->data().descs.size() == 1);
        id = get_resp->data().descs.front()->id;
    }

    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec.value * 1000;
    auto delete_req = std::make_shared<cluster::DeleteTaskRequest>(
        database,
        query.getName(),
        id,
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/1);
    auto delete_resp = metastore.deleteTask(std::move(delete_req));
    if (delete_resp->hasError())
    {
        const auto & err = delete_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_DROP_TASK,
            "Failed to drop task {} in {}: error_code={} error_message={}",
            query.getName(),
            database,
            err.error_code,
            err.error_message);
    }

    return {};
}

}
