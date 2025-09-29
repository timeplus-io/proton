#include <Interpreters/IntrospectionStateLog.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/TimeWheel/TimerService.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Storages/Alert/StorageAlert.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>
#include <Common/timeScale.h>

#if USE_PYTHON_UDF
#include <CPython/AsyncPythonPackageManager.h>
#endif

#include <chrono>


namespace DB
{
namespace
{
bool ignoreDatabase(const String & name)
{
    static const std::unordered_set<std::string> black_list
        = {DatabaseCatalog::TEMPORARY_DATABASE, DatabaseCatalog::INFORMATION_SCHEMA, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE};

    return black_list.contains(name);
}

IntrospectionStateLogElement makeIntrospectionStateLogElement(
    uint64_t node_id,
    const StorageID & storage_id,
    std::string_view state_name,
    UInt64 state_value,
    String state_string_value,
    String dimension)
{
    IntrospectionStateLogElement elem;

    elem.node_id = node_id;
    elem.database = storage_id.getDatabaseName();
    elem.stream_name = storage_id.getTableName();
    elem.uuid = storage_id.uuid;

    elem.state_name = state_name;
    elem.state_value = state_value;
    elem.state_string_value.swap(state_string_value);

    elem.dimension.swap(dimension);

    elem._tp_time = nowSubsecond(3);

    return elem;
}

void addStreamLog(const StorageStream * stream, const AddElem & add_elem)
{
    const auto & storage_id = stream->getStorageID();
    auto logstore_disk_size = stream->getLogStoreDiskSize();
    add_elem(
        storage_id,
        "disk_size",
        logstore_disk_size,
        /*state_string_value=*/"stream",
        /*dimension=*/"log_store");
    add_elem(
        storage_id,
        "disk_size",
        stream->getStorageSize() > logstore_disk_size ? stream->getStorageSize() - logstore_disk_size : 0,
        /*state_string_value=*/"stream",
        /*dimension=*/"historical_store");

    auto shard_applied_sns = stream->appliedSequences();
    for (const auto & shard_applied_sn : shard_applied_sns)
        /// encode shard in `dimension`
        add_elem(
            storage_id,
            "applied_sn",
            (shard_applied_sn.sn >= 0 ? shard_applied_sn.sn : 0),
            /*state_string_value=*/"",
            /*dimension=*/std::to_string(shard_applied_sn.shard));

    auto committed_sns = stream->committedSequences();
    for (const auto & shard_committed_sn : committed_sns)
        /// encode shard in `dimension`
        add_elem(
            storage_id,
            "committed_sn",
            (shard_committed_sn.sn >= 0 ? shard_committed_sn.sn : 0),
            /*state_string_value=*/"",
            /*dimension=*/std::to_string(shard_committed_sn.shard));

    /// stream_index_memory_size;
    /// stream_tail_cache_memory_size;
    /// stream_merge_job_memory_size;
}

void addExternalStreamLog(const StorageExternalStream * stream, const AddElem & add_elem)
{
    /// external_stream_last_consume_sn;
    /// external_stream_last_produce_sn;
    const auto & storage_id = stream->getStorageID();

    add_elem(storage_id, "read_failed", stream->readFailed(true), /*state_string_value=*/"", /*dimension=*/"external_stream");
    add_elem(storage_id, "written_failed", stream->writtenFailed(true), /*state_string_value=*/"", /*dimension=*/"external_stream");
}

void addAlertLog(StorageAlert * alert, const AddElem & add_elem)
{
    const auto & storage_id = alert->getStorageID();

    add_elem(storage_id, "status", 0, /*state_string_value=*/alert->getStatus(), /*dimension=*/"alert");

    if (const auto & last_error_and_ts = alert->getLastError(); last_error_and_ts.first > 0)
        add_elem(
            storage_id,
            "last_error_message",
            last_error_and_ts.first,
            /*state_string_value=*/last_error_and_ts.second,
            /*dimension=*/"alert");

    auto last_trigger = alert->getLastTrigger();
    if (!last_trigger.sns.empty())
        add_elem(storage_id, "processed_sn", 0, /*state_string_value=*/last_trigger.sns.toString(), /*dimension=*/"alert");
    if (last_trigger.rows > 0)
    {
        add_elem(storage_id, "last_trigger_ts", last_trigger.timestamp, /*state_string_value=*/"", /*dimension=*/"alert");
        add_elem(storage_id, "last_trigger_rows", last_trigger.rows, /*state_string_value=*/"", /*dimension=*/"alert");
    }

    if (auto last_throttled_ts = alert->getLastThrottleTimestamp(); last_throttled_ts >= 0)
        add_elem(storage_id, "last_throttled_ts", last_throttled_ts, /*state_string_value=*/"", /*dimension=*/"alert");
}

void addMaterializedViewLog(const StorageMaterializedView * mv, const AddElem & add_elem)
{
    auto metrics = mv->getMetrics();
    const auto * mv_type = "materialized_view";

    const auto & storage_id = mv->getStorageID();
    add_elem(storage_id, "status", 0, /*state_string_value=*/metrics.status, /*dimension=*/mv_type);

    const auto & [last_mv_err, last_ts] = metrics.last_err_msg_and_ts;
    /// encode last_ts in `state_value`
    add_elem(storage_id, "last_error_message", last_ts, /*state_string_value=*/last_mv_err, /*dimension=*/mv_type);
    add_elem(storage_id, "recover_times", metrics.recover_times, /*state_string_value=*/"", /*dimension=*/mv_type);
    add_elem(storage_id, "memory_usage", metrics.memory_usage, /*state_string_value=*/"", /*dimension=*/mv_type);

    /// CPU usage percentage (100% = 1 core, 200% = 2 cores, matches 'top' output)
    /// state_value: Store with 2 decimal precision (1650 = 16.50%)
    /// state_string_value: Store human-readable format (e.g., "16.5%")
    add_elem(
        storage_id,
        "cpu_usage_percent",
        static_cast<UInt64>(metrics.cpu_usage_percentage * 100),
        /*state_string_value=*/fmt::format("{:.1f}%", metrics.cpu_usage_percentage),
        /*dimension=*/mv_type);

    auto pipeline_metrics = mv->getPipelineMetrics();
    if (!pipeline_metrics.empty())
    {
        add_elem(
            storage_id,
            "pipeline",
            /*state_value*/ 0,
            /*state_string_value=*/pipeline_metrics,
            /*dimension=*/mv_type);
    }


    /// Checkpoint metrics
    add_elem(storage_id, "checkpoint_storage_size", metrics.ckpt_storage_size, /*state_string_value=*/"", /*dimension=*/mv_type);
    if (metrics.ckpt_request_metrics)
    {
        add_elem(
            storage_id,
            "last_checkpoint_bytes",
            metrics.ckpt_request_metrics->total_bytes.load(),
            /*state_string_value=*/"",
            /*dimension=*/mv_type);
        add_elem(
            storage_id,
            "last_checkpoint_elapsed_ms",
            metrics.ckpt_request_metrics->stopwatch.elapsedMilliseconds(),
            /*state_string_value=*/"",
            /*dimension=*/mv_type);
    }

    /// Source stream metrics
    for (const auto & s_metrics : metrics.source_metrics)
    {
        const auto & source = s_metrics->description.empty() ? s_metrics->name : s_metrics->description;

        auto [low_sn, high_sn] = s_metrics->low_high_sn_range;
        if (low_sn >= 0)
            add_elem(storage_id, "start_sn", low_sn, /*state_string_value=*/"", /*dimension=*/source);

        /// sequence numbers are int64 while the state value is uint64; so not log negative values such as -1
        auto processed_sn = s_metrics->processed_sn_range.end;
        if (processed_sn > 0)
            add_elem(storage_id, "processed_sn", processed_sn, /*state_string_value=*/"", /*dimension=*/source);
        else
            /// If processed_sn is not there (maybe no new message), set it to low_sn to avoid large process lag.
            /// In some cases, processed_sn can't progress because of first poison events.
            /// To differentiate, we also encode processed_sn in the state_string_value
            add_elem(storage_id, "processed_sn", low_sn, /*state_string_value=*/fmt::format("{}", processed_sn), /*dimension=*/source);

        /// When we don't have high_sn, we treat processed_sn as high_sn for calculating the lagging
        /// sequence range can be stale, for example for Kafka, it is cached and refreshed periodically
        /// so we do `std::max(high_sn, processed_sn)` for end_sn
        if (auto end_sn = std::max(high_sn, processed_sn); end_sn >= 0)
            add_elem(storage_id, "end_sn", end_sn, /*state_string_value=*/"", /*dimension=*/source);

        if (s_metrics->processed_record_ts > 0)
            add_elem(storage_id, "processed_record_ts", s_metrics->processed_record_ts, /*state_string_value=*/"", /*dimension=*/source);

        if (s_metrics->ckpt_sn >= 0)
            add_elem(storage_id, "ckpt_sn", s_metrics->ckpt_sn, /*state_string_value=*/"", /*dimension=*/source);
    }

    /// mv_checkpoint_epoch;

    if (!mv->getExternalTargetTableID())
    {
        if (const auto & inner_storage = mv->tryGetTargetTable())
        {
            if (const auto * inner_storage_stream = dynamic_cast<StorageStream *>(inner_storage.get()))
                addStreamLog(inner_storage_stream, add_elem);
        }
    }
}

void addDictionaryLog(ContextPtr context, const AddElem & add_elem)
{
    const auto & external_dictionaries = context->getExternalDictionariesLoader();
    for (const auto & load_result : external_dictionaries.getLoadResults())
    {
        const auto dict_ptr = std::dynamic_pointer_cast<const IDictionary>(load_result.object);
        if (!dict_ptr)
            continue; /// Ignore not-loaded dictionary metrics

        auto storage_id = dict_ptr->getDictionaryID();
        add_elem(storage_id, "bytes_allocated", dict_ptr->getBytesAllocated(), /*state_string_value=*/"", /*dimension=*/"dict");
        add_elem(
            storage_id,
            "hierarchical_index_bytes_allocated",
            dict_ptr->getHierarchicalIndexBytesAllocated(),
            /*state_string_value=*/"",
            /*dimension=*/"dict");
        add_elem(storage_id, "query_count", dict_ptr->getQueryCount(), /*state_string_value=*/"", /*dimension=*/"dict");
        add_elem(
            storage_id, "hit_rate_pct", static_cast<UInt64>(dict_ptr->getHitRate() * 100), /*state_string_value=*/"", /*dimension=*/"dict");
        add_elem(
            storage_id,
            "found_rate_pct",
            static_cast<UInt64>(dict_ptr->getFoundRate() * 100),
            /*state_string_value=*/"",
            /*dimension=*/"dict");
        add_elem(storage_id, "element_count", dict_ptr->getElementCount(), /*state_string_value=*/"", /*dimension=*/"dict");
        add_elem(
            storage_id,
            "load_factor_pct",
            static_cast<UInt64>(dict_ptr->getLoadFactor() * 100),
            /*state_string_value=*/"",
            /*dimension=*/"dict");

        auto time_since_epoch = [](const auto & time_point) -> UInt64 {
            return static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count());
        };
        add_elem(
            storage_id,
            "loading_start_time",
            time_since_epoch(load_result.loading_start_time),
            /*state_string_value=*/"",
            /*dimension=*/"dict");
        add_elem(
            storage_id,
            "last_successful_update_time",
            time_since_epoch(load_result.last_successful_update_time),
            /*state_string_value=*/"",
            /*dimension=*/"dict");
        add_elem(storage_id, "loading_duration_ms", load_result.loading_duration.count(), /*state_string_value=*/"", /*dimension=*/"dict");

        std::exception_ptr last_exception = load_result.exception ? load_result.exception : dict_ptr->getLastException();
        if (last_exception)
            add_elem(
                storage_id, "last_exception", 0, /*state_string_value=*/getExceptionMessage(last_exception, false), /*dimension=*/"dict");

        add_elem(storage_id, "disk_size", dict_ptr->getStorageSize(), /*state_string_value=*/"", /*dimension=*/"dict");
    }
}

#if USE_PYTHON_UDF
void addPythonPackageLog(ContextPtr context, const AddElem & add_elem)
{
    if (auto python_manager = context->getAsyncPythonPackageManager())
    {
        auto storage_id = StorageID("system", "python_package_manager");

        auto package_metrics = python_manager->getPackageMetrics();

        for (const auto & [package_name, metrics] : package_metrics)
        {
            if (metrics.scheduled_tasks > 0)
                add_elem(storage_id, "scheduled_tasks", metrics.scheduled_tasks, /*state_string_value=*/"", /*dimension=*/package_name);

            if (metrics.installing_tasks > 0)
                add_elem(storage_id, "installing_tasks", metrics.installing_tasks, /*state_string_value=*/"", /*dimension=*/package_name);

            if (metrics.successful_installs > 0)
                add_elem(
                    storage_id,
                    "successful_installs",
                    metrics.successful_installs,
                    /*state_string_value=*/metrics.latest_installed_version,
                    /*dimension=*/package_name);

            if (metrics.failed_installs > 0)
                add_elem(storage_id, "failed_installs", metrics.failed_installs, /*state_string_value=*/"", /*dimension=*/package_name);

            if (metrics.successful_uninstalls > 0)
                add_elem(
                    storage_id,
                    "successful_uninstalls",
                    metrics.successful_uninstalls,
                    /*state_string_value=*/"",
                    /*dimension=*/package_name);

            if (metrics.failed_uninstalls > 0)
                add_elem(storage_id, "failed_uninstalls", metrics.failed_uninstalls, /*state_string_value=*/"", /*dimension=*/package_name);

            if (!metrics.last_error.empty() && metrics.last_error_ts > 0)
                add_elem(
                    storage_id, "last_error", metrics.last_error_ts, /*state_string_value=*/metrics.last_error, /*dimension=*/package_name);
        }
    }
}
#endif
} /// namespace

namespace ErrorCodes
{
extern const int CANNOT_CREATE_TIMER;
}

NamesAndTypesList IntrospectionStateLogElement::getNamesAndTypes()
{
    return {
        {"node_id", std::make_shared<DataTypeUInt64>()},
        {"database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"state_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"state_value", std::make_shared<DataTypeUInt64>()},
        {"state_string_value", std::make_shared<DataTypeString>()},
        {"dimension", std::make_shared<DataTypeString>()},
        {"_tp_time", std::make_shared<DataTypeDateTime64>(3)},
        {"_tp_sn", std::make_shared<DataTypeInt64>()},
    };
}

void IntrospectionStateLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(node_id);
    columns[column_idx++]->insert(database);
    columns[column_idx++]->insert(stream_name);
    columns[column_idx++]->insert(uuid);
    columns[column_idx++]->insert(state_name);
    columns[column_idx++]->insert(state_value);
    columns[column_idx++]->insert(state_string_value);
    columns[column_idx++]->insert(dimension);
    columns[column_idx++]->insert(_tp_time);
}

IntrospectionStateLog::IntrospectionStateLog(
    ContextPtr context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : SystemLog<IntrospectionStateLogElement>(context_, database_name_, table_name_, storage_def_, flush_interval_milliseconds_)
{
    is_force_prepare_tables = true;
}

void IntrospectionStateLog::startCollectStates(int64_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;

    timer_task = getContext()->getTimerService().add(collect_interval_milliseconds, [this]() { collectStates(); }, /*repeat=*/true);
    if (!timer_task)
        throw Exception(ErrorCodes::CANNOT_CREATE_TIMER, "Failed to add collect metrics task to timer");
}

void IntrospectionStateLog::stopCollectStates()
{
    if (stopped.test_and_set())
        return;

    if (timer_task)
        timer_task->cancel();
}

void IntrospectionStateLog::shutdown()
{
    stopCollectStates();
    stopFlushThread();
}

void IntrospectionStateLog::collectStates()
{
    auto local_context = getContext();
    auto node_id = local_context->getNodeID();

    auto add_elem
        = [this, node_id](const StorageID & storage_id, std::string_view name, UInt64 value, String string_value, String dimension) {
              this->add(makeIntrospectionStateLogElement(node_id, storage_id, name, value, std::move(string_value), std::move(dimension)));
          };

    try
    {
        doCollectStates(add_elem, std::move(local_context), log);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to collect introspection states");
    }
}

void IntrospectionStateLog::doCollectStates(AddElem add_elem, ContextPtr local_context, LoggerPtr log_)
{
    /// List all storages [in the specified database]
    Databases databases = DatabaseCatalog::instance().getDatabases();

    uint32_t num_running_mvs = 0, num_running_shards = 0;
    for (const auto & [database_name, database] : databases)
    {
        if (ignoreDatabase(database_name))
            continue;

        if (!database->canContainMergeTreeTables())
            continue;

        for (auto iterator = database->getTablesIterator(local_context); iterator->isValid(); iterator->next())
        {
            try
            {
                const auto & storage = iterator->table();
                if (!storage)
                    continue;

                bool ready = false;
                try
                {
                    ready = storage->isReady();
                }
                catch (...)
                {
                    tryLogCurrentException(log_, "Skip storage due to isReady() failure");
                    continue;
                }

                if (!ready || storage->isVirtualStorage())
                    continue;

                if (const auto * storage_stream = dynamic_cast<StorageStream *>(storage.get()))
                {
                    addStreamLog(storage_stream, add_elem);
                    num_running_shards += static_cast<uint32_t>(storage_stream->getPhysicalShards());
                }
                else if (const auto * storage_external_stream = dynamic_cast<StorageExternalStream *>(storage.get()))
                {
                    addExternalStreamLog(storage_external_stream, add_elem);
                }
                else if (const auto * storage_materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
                {
                    addMaterializedViewLog(storage_materialized_view, add_elem);
                    num_running_mvs += storage_materialized_view->isRunning();
                    if (storage_materialized_view->usesInnerStorage())
                    {
                        if (const auto & inner_storage = storage_materialized_view->tryGetTargetTable())
                        {
                            if (const auto * inner_stream = dynamic_cast<StorageStream *>(inner_storage.get()))
                                num_running_shards += static_cast<uint32_t>(inner_stream->getPhysicalShards());
                        }
                    }
                }
                else if (auto * storage_alert = dynamic_cast<StorageAlert *>(storage.get()))
                {
                    addAlertLog(storage_alert, add_elem);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log_, "Skip storage due to metrics collection failure");
            }
        }
    }

    auto * mutable_context = const_cast<Context *>(local_context.get());
    mutable_context->setTotalMaterializedViews(num_running_mvs);
    mutable_context->setTotalShards(num_running_shards);

    try
    {
        addDictionaryLog(local_context, add_elem);
    }
    catch (...)
    {
        tryLogCurrentException(log_, "AddDictionaryLog failed");
    }

#if USE_PYTHON_UDF
    try
    {
        addPythonPackageLog(local_context, add_elem);
    }
    catch (...)
    {
        tryLogCurrentException(log_, "AddPythonPackageLog failed");
    }
#endif
}

}
