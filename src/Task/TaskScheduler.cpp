#include <Task/TaskScheduler.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/ListTasksResponseData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/ListTasksRequest.h>
#include <Cluster/Requests/ListTasksResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <base/ClockUtils.h>
#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <fmt/ranges.h>


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

        std::scoped_lock lk{tasks_info_mutex};
        for (auto & task : resp->data().descs)
        {
            LOG_INFO(logger, "Task added: {}.{} id={} data_version={}", task->ns, task->name, toString(task->id), task->data_version);
            addOrUpdateTaskUnlocked(std::move(task));
        }

        break;
    }
}

void TaskScheduler::onTaskCreated(const cluster::protocol::TaskDescriptor & task)
{
    {
        auto task_ptr = std::make_shared<cluster::protocol::TaskDescriptor>(task);
        std::scoped_lock lk{tasks_info_mutex};
        addOrUpdateTaskUnlocked(std::move(task_ptr));
    }

    LOG_INFO(logger, "Task added: {}.{} id={} data_version={}", task.ns, task.name, toString(task.id), task.data_version);
}

void TaskScheduler::onTaskDeleted(const UUID & task_id)
{
    {
        std::scoped_lock lk{tasks_info_mutex};
        auto removed = tasks_info.erase(task_id);
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
    auto task_info = std::make_shared<TaskInfo>(task);
    auto [iter, inserted] = tasks_info.insert({task->id, task_info});
    if (!inserted)
    {
        if (iter->second->descriptor->data_version >= task->data_version)
        {
            /// Do not schedule again the task of the same or stale data version
            return;
        }
        iter->second = task_info;
    }

    scheduleTask(task_info);
}

cluster::protocol::TaskDescriptorPtr TaskScheduler::getTaskDescriptor(const UUID & task_id)
{
    std::scoped_lock lk{tasks_info_mutex};
    if (auto iter = deleted_tasks.find(task_id); iter != deleted_tasks.end())
    {
        tasks_info.erase(task_id);
        deleted_tasks.erase(iter);
        return nullptr;
    }

    if (auto iter = tasks_info.find(task_id); iter != tasks_info.end())
        return iter->second->descriptor;

    return nullptr;
}

bool TaskScheduler::isActiveScheduler()
{
    return true;
}

void TaskScheduler::scheduleTask(const TaskInfoPtr & task)
{
    if (stopped.test())
        return;

    /// Check current scheduled tasks to avoid block on timer.add(...)
    if (scheduled_tasks.fetch_add(1) >= max_scheduled_tasks)
    {
        LOG_ERROR(
            logger,
            "Cannot schedule more tasks. Task is removed from scheduling: ns={} name={} max_scheduled_tasks={}",
            task->descriptor->ns,
            task->descriptor->name,
            max_scheduled_tasks);
        --scheduled_tasks;
        return;
    }

    /// TODO: Support cron
    const auto interval_sec = static_cast<Int64>(task->descriptor->interval * task->descriptor->interval_unit.toSeconds());
    const auto current_sec = UTCSeconds::now();
    const auto next_run_sec = (current_sec / interval_sec + 1) * interval_sec;
    const auto schedule_time_sec = std::max(next_run_sec - current_sec, static_cast<Int64>(1));

    LOG_INFO(logger, "Schedule task {}.{} in {} sec.", task->descriptor->ns, task->descriptor->name, schedule_time_sec);

    [[maybe_unused]] auto task_entry = timer.add(
        schedule_time_sec * 1000,
        [this, task, next_run_sec]() {
            /// Make sure execution starts strictly after the scheduled run time to avoid same task scheduled twice due to timer inaccuracy.
            auto now = UTCSeconds::now();
            if (now < next_run_sec)
                sleepForSeconds(next_run_sec - now);

            executeTask(task);
        },
        /*repeat=*/false);
}

bool TaskScheduler::isTaskValid(const cluster::protocol::TaskDescriptorPtr & task)
{
    const auto latest_task_descriptor = getTaskDescriptor(task->id);
    return latest_task_descriptor != nullptr && latest_task_descriptor->data_version == task->data_version;
}

void TaskScheduler::executeTask(const TaskInfoPtr & task)
{
    bool reschedule = true;

    SCOPE_EXIT({
        --scheduled_tasks;
        if (reschedule)
            scheduleTask(task);
    });

    if (!isTaskValid(task->descriptor))
    {
        LOG_DEBUG(logger, "Skip execution as task is removed or staled: task={}.{}", task->descriptor->ns, task->descriptor->name);
        reschedule = false;
        return;
    }

    if (!isActiveScheduler())
    {
        LOG_DEBUG(logger, "Skip task execution on inactive scheduler: task={}.{}", task->descriptor->ns, task->descriptor->name);
        reschedule = true;
        return;
    }

    LOG_INFO(logger, "Task execution start: task={}.{} local=true", task->descriptor->ns, task->descriptor->name);
    try
    {
        std::string query = fmt::format("SYSTEM EXECUTE TASK {}.{}", task->descriptor->ns, task->descriptor->name);
        auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("");

        CurrentThread::QueryScope query_scope{query_context};
        executeNonInsertQuery(query, query_context, /*callback=*/{}, /*internal=*/false);

        LOG_INFO(logger, "Task execution end: task={}.{}", task->descriptor->ns, task->descriptor->name);
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Task execution fail: task={}.{} error={}", task->descriptor->ns, task->descriptor->name, ex.message());
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}

std::vector<TaskInfoPtr> TaskScheduler::getTasks(const std::string & ns) const
{
    std::vector<TaskInfoPtr> tasks;

    std::scoped_lock lk{tasks_info_mutex};
    for (const auto & [id, task] : tasks_info)
    {
        if (!deleted_tasks.contains(id))
        {
            if (ns.empty() || task->descriptor->ns == ns)
                tasks.push_back(task);
        }
    }

    return tasks;
}
}
