#include "TaskRestRouterHandler.h"

#include <common/DateLUT.h>


namespace DB
{
namespace
{
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

String TaskRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    const auto & task_id = getPathParameter("task_id");

    Poco::JSON::Array tasks_array;
    auto & task_service = TaskStatusService::instance(query_context);

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
        http_status = 404;
        return "Not Found";
    }

    Poco::JSON::Object result;
    result.set("query_id", query_context->getCurrentQueryId());
    result.set("tasks", tasks_array);

    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::condense(result, oss);
    return oss.str();
}

};
