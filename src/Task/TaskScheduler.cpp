#include <Task/TaskScheduler.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/ListTasksResponseData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/ListTasksRequest.h>
#include <Cluster/Requests/ListTasksResponse.h>
#include <Task/TaskExecution.h>
#include <Task/TaskExecutionState.h>
#include <base/ClockUtils.h>
#include <base/sleep.h>


namespace DB::Task
{

TaskScheduler::TaskScheduler(size_t worker_threads, size_t max_scheduled_tasks_)
    : max_scheduled_tasks(max_scheduled_tasks_)
    , timer(worker_threads, /*tick_ms=*/1000, /*wheel_size=*/6000, max_scheduled_tasks)
    , logger(getLogger("TaskScheduler"))
{
}

TaskScheduler::~TaskScheduler()
{
    shutdown();
}

void TaskScheduler::startup()
{
    if (started.test_and_set())
        return;

    [[maybe_unused]] auto load_task = timer.add(0, [this]() { loadTaskDescriptors(); }, /*repeat=*/false);
    LOG_INFO(logger, "Started");
}

void TaskScheduler::shutdown()
{
    if (stopped.test_and_set())
        return;

    timer.shutdown();

    LOG_INFO(logger, "Stopped");
}

void TaskScheduler::loadTaskDescriptors()
{
    while (!stopped.test())
    {
        auto & meta_store = Globals::getMetaStore();
        auto req = std::make_shared<cluster::ListTasksRequest>(1);
        req->data().ns = "";
        auto resp = meta_store.listTasks(req);

        if (resp->hasError())
        {
            LOG_ERROR(logger, "Failed to load task descriptors: {}", resp->error().string());
            sleepForSeconds(2);
            continue;
        }

        std::scoped_lock lk{task_descriptors_mutex};
        for (auto & task : resp->data().descs)
        {
            addOrUpdateTaskUnlocked(std::move(task));
        }

        break;
    }
}

void TaskScheduler::onTaskCreated(const cluster::protocol::TaskDescriptor & task)
{
    {
        auto task_ptr = std::make_shared<cluster::protocol::TaskDescriptor>(task);
        std::scoped_lock lk{task_descriptors_mutex};
        addOrUpdateTaskUnlocked(std::move(task_ptr));
    }

    LOG_INFO(logger, "Task added: {}.{} id={} data_version={}", task.ns, task.name, toString(task.id), task.data_version);
}

void TaskScheduler::onTaskDeleted(const UUID & task_id)
{
    {
        std::scoped_lock lk{task_descriptors_mutex};
        auto removed = task_descriptors.erase(task_id);
        if (removed == 0)
        {
            /// In case task_descriptors add removed task later (from loadTaskDescriptors)
            deleted_tasks.emplace(task_id);
        }
    }

    LOG_INFO(logger, "Task removed: id={}", toString(task_id));
}

void TaskScheduler::addOrUpdateTaskUnlocked(cluster::protocol::TaskDescriptorPtr task)
{
    auto [iter, inserted] = task_descriptors.insert({task->id, task});
    if (!inserted && iter->second->data_version < task->data_version)
    {
        iter->second = task;
    }

    scheduleTask(*task);
}

cluster::protocol::TaskDescriptorPtr TaskScheduler::getTaskDescriptor(const UUID & task_id)
{
    std::scoped_lock lk{task_descriptors_mutex};
    if (auto iter = deleted_tasks.find(task_id); iter != deleted_tasks.end())
    {
        task_descriptors.erase(task_id);
        deleted_tasks.erase(iter);
        return nullptr;
    }

    if (auto iter = task_descriptors.find(task_id); iter != task_descriptors.end())
        return iter->second;

    return nullptr;
}

bool TaskScheduler::isExecutor()
{
    return true;
}

void TaskScheduler::scheduleTask(const cluster::protocol::TaskDescriptor & task)
{
    if (stopped.test())
        return;

    /// Check current scheduled tasks to avoid block on timer.add(...)
    if (scheduled_tasks.fetch_add(1) >= max_scheduled_tasks)
    {
        LOG_ERROR(
            logger,
            "Cannot schedule more tasks. Task is removed from scheduling: ns={} name={} max_scheduled_tasks={}",
            task.ns,
            task.name,
            max_scheduled_tasks);
        --scheduled_tasks;
        return;
    }

    /// TODO: Support cron
    const auto interval_sec = static_cast<Int64>(task.interval * task.interval_unit.toSeconds());
    const auto current_sec = UTCSeconds::now();
    const auto next_run_sec = (current_sec / interval_sec + 1) * interval_sec;
    const auto schedule_time_sec = std::max(next_run_sec - current_sec, static_cast<Int64>(1));

    LOG_INFO(logger, "Schedule task {}.{} in {} sec.", task.ns, task.name, schedule_time_sec);

    auto task_exec = std::make_shared<TaskExecution>(task.id, task.data_version, shared_from_this(), next_run_sec);
    [[maybe_unused]] auto task_entry = timer.add(
        schedule_time_sec * 1000,
        [task_exec_ = std::move(task_exec)]() { task_exec_->execute(); },
        /*repeat=*/false);
}

void TaskScheduler::onTaskExecutionComplete(const cluster::protocol::TaskDescriptorPtr & task, std::optional<TaskExecutionState> state)
{
    if (state)
    {
        chassert(task);
        auto error = saveTaskExecutionState(task->id, task->data_version, *state);
        if (error != ErrorCodes::OK)
            LOG_ERROR(
                logger, "Failed to save task execution state: id={} version={} error={}", toString(task->id), task->data_version, error);
    }

    --scheduled_tasks;
    if (task)
    {
        scheduleTask(*task);
    }
}

cluster::protocol::TaskDescriptorPtrs TaskScheduler::getTasks(const std::string & ns) const
{
    cluster::protocol::TaskDescriptorPtrs tasks;

    std::scoped_lock lk{task_descriptors_mutex};
    for (const auto & [id, task] : task_descriptors)
    {
        if (!deleted_tasks.contains(id))
        {
            if (ns.empty() || task->ns == ns)
                tasks.push_back(task);
        }
    }

    return tasks;
}
}
