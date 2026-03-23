#pragma once

#include <Task/TaskExecutionResult.h>
#include <Task/TaskInfo.h>

#include <memory>
#include <string>
#include <vector>


namespace DB
{

class ASTCreateTaskQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

namespace Task
{
std::vector<TaskInfoPtr> getTaskInfos(const std::string & ns);
std::string getTaskQuery(const std::string & sql, const Checkpoint & checkpoint);

void validateCreateTaskQuery(const ASTCreateTaskQuery * create_task_query, const ContextPtr & context);
}

}
