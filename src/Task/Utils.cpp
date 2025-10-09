#include <Task/Utils.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/SelectQueryDescription.h>
#include <Task/TaskScheduler.h>

#include <ranges>


namespace DB::Task
{

std::string getTaskCompleteQuery(const std::string & sql, const Checkpoint & checkpoint)
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

std::vector<TaskInfo> getTaskInfos(const std::string & ns)
{
    const auto task_scheduler = Globals::getGlobalContext().getTaskScheduler();
    chassert(task_scheduler);

    auto tasks = task_scheduler->getTasks(ns);

    std::vector<TaskInfo> task_infos;
    task_infos.reserve(tasks.size());

    for (const auto & task : tasks)
    {
        TaskInfo task_info{StorageID{task->ns, task->name, task->id}, task->status};

        if (!task->target_table_name.empty())
            task_info.target_table = StorageID{task->target_database_name, task->target_table_name};

        /// Get source tables
        auto maybe_checkpoint = parseCheckpoint(task->checkpoint_init_values);
        if (maybe_checkpoint.hasError())
            continue;

        auto source_query = getTaskCompleteQuery(task->sql, maybe_checkpoint.result);
        ParserQuery parser(source_query.data() + source_query.size());
        auto select_ast = parseQuery(
            parser,
            source_query.data(),
            source_query.data() + source_query.size(),
            "",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (const auto * select = select_ast->as<ASTSelectWithUnionQuery>(); select != nullptr)
        {
            auto description
                = SelectQueryDescription::getSelectQueryFromASTForView(select_ast, Globals::getGlobalContext().getGlobalContext());
            task_info.source_tables = std::move(description.select_table_ids);
        }

        task_infos.push_back(std::move(task_info));
    }

    return task_infos;
}

}
