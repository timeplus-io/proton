#include <Task/Utils.h>

#include <Bootstrap/Globals.h>
#include <Interpreters/Context.h>
#include <Task/TaskScheduler.h>

#include <ranges>


namespace DB::Task
{

std::string getTaskQuery(const std::string & sql, const Checkpoint & checkpoint)
{
    auto complete_query = sql;

    /// Fill query with checkpoint values
    for (const auto & [k, v] : checkpoint)
    {
        auto token = fmt::format("${{{}}}", k);
        size_t start_pos = 0;
        while ((start_pos = sql.find(token, start_pos)) != std::string::npos)
        {
            complete_query.replace(start_pos, token.length(), v);
            start_pos += v.length();
        }
    }

    return complete_query;
}

std::vector<TaskInfoPtr> getTaskInfos(const std::string & ns)
{
    const auto task_scheduler = Globals::getGlobalContext().getTaskScheduler();
    chassert(task_scheduler);
    return task_scheduler->getTasks(ns);
}

}
