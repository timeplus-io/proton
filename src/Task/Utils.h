#pragma once

#include <Task/TaskExecutionResult.h>
#include <Task/TaskInfo.h>

#include <string>
#include <vector>


namespace DB::Task
{
std::vector<TaskInfoPtr> getTaskInfos(const std::string & ns);
std::string getTaskQuery(const std::string & sql, const Checkpoint & checkpoint);
}
