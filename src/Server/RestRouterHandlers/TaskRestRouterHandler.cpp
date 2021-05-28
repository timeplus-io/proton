#include "TaskRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>
#include <DistributedMetadata/TaskStatusService.h>
#include <DistributedMetadata/sendRequest.h>

#include <common/DateLUT.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_NOT_INITED;
}

namespace
{
    const String TASK_URL = "http://{}:{}/dae/v1/tasks/{}";

    Poco::JSON::Array buildTaskResponse(const std::vector<DB::TaskStatusService::TaskStatusPtr> & tasks)
    {
        Poco::JSON::Array task_list;
        for (const auto & task : tasks)
        {
            Poco::JSON::Object task_object;
            task_object.set("id", task->id);
            task_object.set("status", task->status);
            task_object.set("progress", task->progress);
            task_object.set("reason", task->reason);
            task_object.set("user", task->user);
            task_object.set("context", task->context);
            task_object.set("last_modified", task->last_modified);
            task_object.set("created", task->created);

            task_list.add(task_object);
        }
        return task_list;
    }
}

std::pair<String, Int32> TaskRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const auto & node_roles = CatalogService::instance(query_context).nodeRoles();
    if (node_roles.find("task") != String::npos)
    {
        /// Current node has TaskService running. Handle it locally
        return getTasksLocally();
    }
    else
    {
        auto nodes{CatalogService::instance(query_context).nodes("task")};

        if (nodes.empty())
        {
            return {jsonErrorResponse("Internal server error", ErrorCodes::RESOURCE_NOT_INITED), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
        }

        /// FIXME, https
        /// Forward the request to TaskService node
        Poco::URI uri{fmt::format(TASK_URL, nodes[0]->host, nodes[0]->http_port, getPathParameter("task_id"))};
        auto [response, http_status] = sendRequest(
            uri,
            Poco::Net::HTTPRequest::HTTP_GET,
            query_context->getCurrentQueryId(),
            query_context->getUserName(),
            query_context->getPasswordByUserName(query_context->getUserName()),
            "",
            log);

        if (http_status == HTTPResponse::HTTP_OK)
        {
            return {response, http_status};
        }

        return {jsonErrorResponseFrom(response), http_status};
    }
}

std::pair<String, Int32> TaskRestRouterHandler::getTasksLocally() const
{
    auto & task_service = TaskStatusService::instance(query_context);

    Poco::JSON::Array tasks_array;

    const auto & task_id = getPathParameter("task_id");
    if (task_id.empty())
    {
        const auto & user = query_context->getUserName();
        auto tasks = task_service.findByUser(user);
        tasks_array = buildTaskResponse(tasks);
    }
    else if (!task_id.empty())
    {
        auto task = task_service.findById(task_id);
        if (task != nullptr)
        {
            std::vector<TaskStatusService::TaskStatusPtr> tasks{task};
            tasks_array = buildTaskResponse(tasks);
        }
    }
    else
    {
        return {jsonErrorResponse("Not found", ErrorCodes::RESOURCE_NOT_FOUND), HTTPResponse::HTTP_NOT_FOUND};
    }

    Poco::JSON::Object result;
    result.set("request_id", query_context->getCurrentQueryId());
    result.set("tasks", tasks_array);

    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::condense(result, oss);
    return {oss.str(), HTTPResponse::HTTP_OK};
}
};
