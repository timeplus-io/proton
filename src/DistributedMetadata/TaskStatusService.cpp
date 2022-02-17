#include "TaskStatusService.h"
#include "CatalogService.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DistributedWALClient/KafkaWALCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/BlockUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/DateLUT.h>

#include <Poco/Util/Application.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{
    String TASK_KEY_PREFIX = "cluster_settings.system_tasks.";
    String TASK_DEFAULT_TOPIC = "__system_tasks";

    Block buildBlock(const std::vector<TaskStatusService::TaskStatusPtr> & tasks)
    {
        std::vector<std::pair<String, std::vector<String>>> string_cols
            = {{"id", std::vector<String>()},
               {"status", std::vector<String>()},
               {"progress", std::vector<String>()},
               {"reason", std::vector<String>()},
               {"user", std::vector<String>()},
               {"context", std::vector<String>()}};

        std::vector<std::pair<String, std::vector<Int64>>> int64_cols
            = {{"last_modified", std::vector<Int64>()}, {"created", std::vector<Int64>()}};

        for (const auto & task : tasks)
        {
            for (auto & col : string_cols)
            {
                if ("id" == col.first)
                {
                    col.second.push_back(task->id);
                }
                else if ("status" == col.first)
                {
                    col.second.push_back(task->status);
                }
                else if ("progress" == col.first)
                {
                    col.second.push_back(task->progress);
                }
                else if ("reason" == col.first)
                {
                    col.second.push_back(task->reason);
                }
                else if ("user" == col.first)
                {
                    col.second.push_back(task->user);
                }
                else if ("context" == col.first)
                {
                    col.second.push_back(task->context);
                }
                else
                {
                    assert(false);
                }
            }

            for (auto & col : int64_cols)
            {
                if ("last_modified" == col.first)
                {
                    col.second.push_back(task->last_modified == -1 ? (UTCMilliseconds::now()) : task->last_modified);
                }
                else if ("created" == col.first)
                {
                    col.second.push_back(task->created == -1 ? (UTCMilliseconds::now()) : task->created);
                }
                else
                {
                    assert(false);
                }
            }
        }

        return DB::buildBlock(string_cols, int64_cols);
    }

    DWAL::Record buildRecord(const TaskStatusService::TaskStatusPtr & task)
    {
        std::vector<TaskStatusService::TaskStatusPtr> tasks = {task};
        auto block = buildBlock(tasks);
        return DWAL::Record(DWAL::OpCode::ADD_DATA_BLOCK, std::move(block), DWAL::NO_SCHEMA);
    }

    /// Escape input string to make it legal to `INSERT INTO`
    String & escapeValue(String & s)
    {
        std::replace(s.begin(), s.end(), '\'', ' ');
        return s;
    }
}

const String TaskStatusService::TaskStatus::SUBMITTED = "SUBMITTED";
const String TaskStatusService::TaskStatus::INPROGRESS = "INPROGRESS";
const String TaskStatusService::TaskStatus::SUCCEEDED = "SUCCEEDED";
const String TaskStatusService::TaskStatus::FAILED = "FAILED";


TaskStatusService & TaskStatusService::instance(const ContextMutablePtr & global_context_)
{
    static TaskStatusService task_status_service{global_context_};
    return task_status_service;
}

TaskStatusService::TaskStatusService(const ContextMutablePtr & global_context_) : MetadataService(global_context_, "TaskStatusService")
{
    const auto & config = global_context->getConfigRef();
    const auto & conf = configSettings();
    max_cached_size_threshold = config.getUInt64(conf.key_prefix + "max_cached_size_threshold", 100000);
}

MetadataService::ConfigSettings TaskStatusService::configSettings() const
{
    return {
        .key_prefix = TASK_KEY_PREFIX,
        .default_name = TASK_DEFAULT_TOPIC,
        .default_data_retention = 24,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

Int32 TaskStatusService::append(TaskStatusPtr task)
{
    const auto & result = appendRecord(buildRecord(task));
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to commit task {}", task->id);
        return result.err;
    }
    return 0;
}

void TaskStatusService::processRecords(const DWAL::RecordPtrs & records)
{
    /// Consume records and build in-memory indexes
    std::vector<TaskStatusPtr> tasks;
    tasks.reserve(records.size());
    for (const auto & record : records)
    {
        assert(!record->hasSchema());
        assert(record->op_code == DWAL::OpCode::ADD_DATA_BLOCK);

        auto task = buildTaskStatusFromRecord(record);
        if (task)
        {
            updateTaskStatus(task);
            tasks.emplace_back(std::move(task));
        }
    }

    while (!table_exists)
    {
        /// Wait for system.tasks creation
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    /// FIXME: Checkpointing
    persistentTaskStatuses(std::move(tasks));
    cleanupCachedTask();
}

void TaskStatusService::updateTaskStatus(TaskStatusPtr & task_ptr)
{
    std::unique_lock guard(indexes_lock);

    if (task_ptr->status == TaskStatus::SUCCEEDED || task_ptr->status == TaskStatus::FAILED)
        finished_tasks.push_back(task_ptr);

    auto id_map_iter = indexed_by_id.find(task_ptr->id);
    if (id_map_iter == indexed_by_id.end())
    {
        indexed_by_id[task_ptr->id] = task_ptr;
        auto user_map_iter = indexed_by_user.find(task_ptr->user);
        if (user_map_iter == indexed_by_user.end())
        {
            indexed_by_user[task_ptr->user] = std::unordered_map<String, TaskStatusPtr>();
            user_map_iter = indexed_by_user.find(task_ptr->user);
        }

        assert(user_map_iter != indexed_by_user.end());
        assert(!user_map_iter->second.contains(task_ptr->id));

        user_map_iter->second[task_ptr->id] = task_ptr;
        return;
    }

    assert(id_map_iter->second->id == task_ptr->id);

    /// Do not copy created field to an exist task
    id_map_iter->second->status = task_ptr->status;
    id_map_iter->second->progress = task_ptr->progress;
    id_map_iter->second->reason = task_ptr->reason;
    id_map_iter->second->user = task_ptr->user;
    id_map_iter->second->context = task_ptr->context;
    id_map_iter->second->last_modified = task_ptr->last_modified;

    /// Assert pointer in id_map and pointer in user_map are always point to the same object
    auto user_map_iter = indexed_by_user.find(task_ptr->user);
    assert(user_map_iter != indexed_by_user.end());

    auto task_in_user_map_iter = user_map_iter->second.find(task_ptr->id);
    (void)task_in_user_map_iter;
    assert(task_in_user_map_iter != user_map_iter->second.end());
    assert(id_map_iter->second.get() == task_in_user_map_iter->second.get());
}

bool TaskStatusService::validateSchema(const Block & block, const std::vector<String> & col_names) const
{
    for (const auto & col_name : col_names)
    {
        if (!block.has(col_name))
        {
            LOG_ERROR(log, "`{}` column is missing", col_name);
            return false;
        }
    }
    return true;
}

bool TaskStatusService::tableExists()
{
    if (table_exists)
        return true;

    StorageID sid{"system", "tasks"};
    /// Try local catalog
    if (DatabaseCatalog::instance().isTableExist(sid, global_context))
    {
        table_exists = true;
        return true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    /// Try catalog service
    auto & catalog_service = CatalogService::instance(global_context);
    if (catalog_service.tableExists(sid.getDatabaseName(), sid.getTableName()))
    {
        table_exists = true;
        return true;
    }

    ///std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    /// Try topic name
    ///auto result = dwal->describe(DWAL::escapeDWALName("system", "tasks"), dwal_append_ctx);
    ///table_exists = (result.err == ErrorCodes::OK);
    ///return table_exists;
    return false;
}

TaskStatusService::TaskStatusPtr TaskStatusService::buildTaskStatusFromRecord(const DWAL::RecordPtr & record) const
{
    std::vector<TaskStatusService::TaskStatusPtr> tasks;
    buildTaskStatusFromBlock(record->block, tasks);
    if (tasks.empty())
        return nullptr;

    return tasks[0];
}

void TaskStatusService::buildTaskStatusFromBlock(const Block & block, std::vector<TaskStatusService::TaskStatusPtr> & res) const
{
    if (!validateSchema(block, {"id", "status", "progress", "reason", "user", "context", "last_modified", "created"}))
        return;

    const auto & id_col = block.findByName("id")->column;
    const auto & status_col = block.findByName("status")->column;
    const auto & progress_col = block.findByName("progress")->column;
    const auto & reason_col = block.findByName("reason")->column;
    const auto & user_col = block.findByName("user")->column;
    const auto & context_col = block.findByName("context")->column;
    const auto & last_modified_col = block.findByName("last_modified")->column;
    const auto & created_col = block.findByName("created")->column;

    for (size_t i = 0; i < id_col->size(); ++i)
    {
        auto task = std::make_shared<TaskStatus>();
        task->id = id_col->getDataAt(i).toString();
        task->status = status_col->getDataAt(i).toString();
        task->progress = progress_col->getDataAt(i).toString();
        task->reason = reason_col->getDataAt(i).toString();
        task->user = user_col->getDataAt(i).toString();
        task->context = context_col->getDataAt(i).toString();
        task->last_modified = last_modified_col->getInt(i);
        task->created = created_col->getInt(i);

        assert(task->last_modified);
        assert(task->created);

        res.push_back(task);
    }
}

TaskStatusService::TaskStatusPtr TaskStatusService::findById(const String & id)
{
    if (auto task_ptr = findByIdInMemory(id))
    {
        return task_ptr;
    }
    return findByIdInTable(id);
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInMemory(const String & id)
{
    std::shared_lock guard(indexes_lock);
    if (auto task = indexed_by_id.find(id); task != indexed_by_id.end())
    {
        return task->second;
    }
    return nullptr;
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInTable(const String & id)
{
    if (!tableExists())
    {
        return nullptr;
    }

    CurrentThread::detachQueryIfNotDetached();
    /// FIXME: Remove DISTINCT when we resolve the checkpointing issue
    constexpr auto * query_template = "SELECT DISTINCT id, status, progress, "
                                      "reason, user, context, created, last_modified "
                                      "FROM table(system.tasks) "
                                      "WHERE user <> '' AND id == '{}' "
                                      "ORDER BY last_modified DESC";
    auto query = fmt::format(query_template, id);

    std::vector<TaskStatusService::TaskStatusPtr> res;

    auto query_context = Context::createCopy(global_context);
    CurrentThread::QueryScope query_scope{query_context};

    executeNonInsertQuery(query, query_context, [this, &res](Block && block) { this->buildTaskStatusFromBlock(block, res); });
    if (res.empty())
        return nullptr;
    return res[0];
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::findByUser(const String & user)
{
    std::vector<TaskStatusService::TaskStatusPtr> res;
    findByUserInMemory(user, res);
    findByUserInTable(user, res);

    return res;
}

void TaskStatusService::findByUserInMemory(const String & user, std::vector<TaskStatusService::TaskStatusPtr> & res)
{
    std::shared_lock guard(indexes_lock);
    auto user_map_iter = indexed_by_user.find(user);
    if (user_map_iter == indexed_by_user.end())
    {
        return;
    }

    for (auto it = user_map_iter->second.begin(); it != user_map_iter->second.end(); ++it)
    {
        res.push_back(it->second);
    }
}

void TaskStatusService::findByUserInTable(const String & user, std::vector<TaskStatusService::TaskStatusPtr> & res)
{
    if (!tableExists())
    {
        return;
    }

    assert(!user.empty());
    CurrentThread::detachQueryIfNotDetached();
    constexpr auto * query_template = "SELECT DISTINCT id, status, progress, "
                                      "reason, user, context, created, last_modified "
                                      "FROM table(system.tasks) "
                                      "WHERE user == '{}' "
                                      "ORDER BY last_modified DESC";
    auto query = fmt::format(query_template, user);

    auto query_context = Context::createCopy(global_context);
    CurrentThread::QueryScope query_scope{query_context};

    executeNonInsertQuery(query, query_context, [this, &res](Block && block) { this->buildTaskStatusFromBlock(block, res); });
}

void TaskStatusService::createTaskTableIfNotExists()
{
    if (!tableExists())
    {
        for (int i = 0; i < RETRY_TIMES; ++i)
        {
            if (createTaskTable())
                break;

            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
        }

        if (!tableExists())
            throw Exception("Failed to create system tasks table", ErrorCodes::UNKNOWN_EXCEPTION);
    }
}

void TaskStatusService::cleanupCachedTask()
{
    try
    {
        doCleanupCachedTask();
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Clean up cached task failed: {}", e.message());
    }
    catch (...)
    {
        LOG_ERROR(log, "Clean up cached task failed: ", getCurrentExceptionMessage(true, true));
    }
}

void TaskStatusService::doCleanupCachedTask()
{
    //// Remove over max size threshold tasks
    std::unique_lock guard(indexes_lock);
    if (indexed_by_id.size() <= max_cached_size_threshold)
        return;

    /// Remove finished task
    for(const auto & task_ptr : finished_tasks)
        cleanupTaskByIdUnlocked(task_ptr->id);

    finished_tasks.clear();

    /// In some case: non-finished tasks still are more than max_cached_size_threshold
    /// We remove the part of tasks that overflows max_cached_size_threshold
    while (indexed_by_id.size() > max_cached_size_threshold)
        cleanupTaskByIdUnlocked(indexed_by_id.begin()->second->id);
}

void TaskStatusService::cleanupTaskByIdUnlocked(const String & id)
{
    auto id_map_iter = indexed_by_id.find(id);
    assert(id_map_iter != indexed_by_id.end());
    const auto & user = id_map_iter->second->user;

    /// Remove task from indexed_by_user map
    auto user_map_iter = indexed_by_user.find(user);
    assert(user_map_iter != indexed_by_user.end());

    auto task_iter = user_map_iter->second.find(id);
    assert(task_iter != user_map_iter->second.end());
    user_map_iter->second.erase(task_iter);
    assert(user_map_iter->second.find(id) == user_map_iter->second.end());
    if (user_map_iter->second.empty())
    {
        indexed_by_user.erase(user_map_iter);
        assert(indexed_by_user.find(user) == indexed_by_user.end());
    }

    /// Remove task from indexed_by_user map
    indexed_by_id.erase(id_map_iter);
}

bool TaskStatusService::createTaskTable()
{
    const auto & config = global_context->getConfigRef();
    const auto & conf = configSettings();
    const String replication_factor_key = conf.key_prefix + "replication_factor";
    const auto replicas = std::to_string(config.getInt(replication_factor_key, 1));

    String query = fmt::format(
        "CREATE STREAM IF NOT EXISTS \
                    system.tasks \
                    ( \
                    `id` String, \
                    `status` String, \
                    `progress` String, \
                    `reason` String, \
                    `user` String, \
                    `context` String, \
                    `created` Int64, \
                    `last_modified` Int64, \
                    `_tp_time` DateTime64(3, 'UTC') DEFAULT from_unix_timestamp64_milli(created, 'UTC'), \
                    `_tp_index_time` DateTime64(3, 'UTC') \
                     ) \
                    ENGINE = DistributedMergeTree(1,{},rand()) \
                    ORDER BY (to_minute(_tp_time), user, id) \
                    PARTITION BY to_date(_tp_time) \
                    TTL to_datetime(_tp_time + to_interval_day(7)) DELETE \
                    SETTINGS index_granularity=8192",
        replicas);

    auto query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    query_context->setUserByName("system");
    query_context->setDistributedDDLOperation(true);
    query_context->setSetting("synchronous_ddl", true);
    /// CurrentThread::QueryScope query_scope{query_context};

    try
    {
        executeNonInsertQuery(
            query, query_context, [](Block &&) {}, true);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Create task table failed: {}", e.message());
        return false;
    }
    catch (...)
    {
        LOG_ERROR(log, "Create task table failed. ", getCurrentExceptionMessage(true, true));
        return false;
    }

    /// Poll if table creation succeeds.
    return tableExists();
}

bool TaskStatusService::persistentTaskStatuses(std::vector<TaskStatusPtr> tasks)
{
    assert(!tasks.empty());

    auto context = Context::createCopy(global_context);
    context->setCurrentQueryId("");
    CurrentThread::QueryScope query_scope{context};

    String query = "INSERT INTO system.tasks \
                    (id, status, progress, reason, user, context, created, last_modified) \
                    VALUES ";

    std::vector<String> values;
    values.reserve(tasks.size());

    for (const auto & task : tasks)
    {
        auto created = std::to_string(task->created == -1 ? (UTCMilliseconds::now()) : task->created);
        auto last_modified = std::to_string(task->last_modified == -1 ? (UTCMilliseconds::now()) : task->last_modified);
        values.push_back(fmt::format(
            "('{}', '{}', '{}', '{}', '{}', '{}', {}, {})",
            task->id,
            task->status,
            task->progress,
            escapeValue(task->reason),
            escapeValue(task->user),
            escapeValue(task->context),
            created,
            last_modified));
    }

    query += boost::algorithm::join(values,  ", ");

    LOG_INFO(log, "Persistent {} tasks. ", tasks.size());
    ReadBufferFromString in(query);
    String dummy_string;
    WriteBufferFromString out(dummy_string);

    try
    {
        executeQuery(in, out, /* allow_into_outfile = */ false, context, {});
    }
    catch (...)
    {
        LOG_ERROR(log, "Persistent task failed", getCurrentExceptionMessage(true, true));
        return false;
    }
    return true;
}

}
