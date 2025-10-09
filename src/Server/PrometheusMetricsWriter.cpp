#include "PrometheusMetricsWriter.h"

#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/Alert/StorageAlert.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Common/StatusInfo.h>
#include <Common/re2.h>

#include <algorithm>

namespace
{

template <typename T>
void writeOutLine(DB::WriteBuffer & wb, T && val)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar('\n', wb);
}

template <typename T, typename... TArgs>
void writeOutLine(DB::WriteBuffer & wb, T && val, TArgs &&... args)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar(' ', wb);
    writeOutLine(wb, std::forward<TArgs>(args)...);
}

/// Returns false if name is not valid
bool replaceInvalidChars(std::string & metric_name)
{
    /// dirty solution:
    static const re2::RE2 regexp1("[^a-zA-Z0-9_:]");
    static const re2::RE2 regexp2("^[^a-zA-Z]*");
    re2::RE2::GlobalReplace(&metric_name, regexp1, "_");
    re2::RE2::GlobalReplace(&metric_name, regexp2, "");
    return !metric_name.empty();
}


template <typename T>
void writeQueryMetrics(
    DB::WriteBuffer & wb,
    const std::string & prefix,
    const std::string & key,
    const std::string & help_msg,
    const std::string & type,
    const std::string & query_id,
    const std::string & creator,
    T && value)
{
    std::string metric_name = prefix + key;
    writeOutLine(wb, "# HELP", metric_name, help_msg);
    writeOutLine(wb, "# TYPE", metric_name, type);
    DB::writeText(metric_name, wb);
    DB::writeText("{id=\"", wb);
    DB::writeText(query_id, wb);
    DB::writeText("\", materialized_view=\"", wb);
    DB::writeText(creator, wb);
    DB::writeText("\"} ", wb);
    writeOutLine(wb, std::forward<T>(value));
}

void writeQueryInfo(DB::WriteBuffer & wb, const std::string & prefix, const DB::QueryStatusInfo & query_info)
{
    const auto & query_id = query_info.client_info.current_query_id;

    writeQueryMetrics(
        wb,
        prefix,
        "ScanEvents",
        "Total number of events the query has scanned",
        "counter",
        query_id,
        query_info.creator,
        query_info.read_rows);
    writeQueryMetrics(
        wb, prefix, "ScanBytes", "Total bytes the query has scanned", "counter", query_id, query_info.creator, query_info.read_bytes);
    writeQueryMetrics(
        wb,
        prefix,
        "OutputEvents",
        "Total number of events the query has outputted",
        "counter",
        query_id,
        query_info.creator,
        query_info.written_rows);
    writeQueryMetrics(
        wb,
        prefix,
        "OutputBytes",
        "Total bytes the query has outputted",
        "counter",
        query_id,
        query_info.creator,
        query_info.written_bytes);
    writeQueryMetrics(
        wb,
        prefix,
        "MemoryUsageBytes",
        "Size of memory the query has consumed",
        "gauge",
        query_id,
        query_info.creator,
        query_info.memory_usage);
    writeQueryMetrics(
        wb,
        prefix,
        "PeakMemoryUsageBytes",
        "Peak size of memory the query has consumed",
        "gauge",
        query_id,
        query_info.creator,
        query_info.peak_memory_usage);
    writeQueryMetrics(
        wb,
        prefix,
        "ElapsedMicroseconds",
        "Total time the query has been running",
        "counter",
        query_id,
        query_info.creator,
        static_cast<UInt64>(query_info.elapsed_microseconds));
}

template <typename T>
void writeExternalStreamMetrics(
    DB::WriteBuffer & wb,
    const std::string & prefix,
    const std::string & key,
    const std::string & type,
    const std::string & database_name,
    const std::string & table_name,
    T && value)
{
    std::string metric_name{prefix + key};
    writeOutLine(wb, "# TYPE", metric_name, type);
    DB::writeText(metric_name, wb);
    DB::writeText("{database=\"", wb);
    DB::writeText(database_name, wb);
    DB::writeText("\", name=\"", wb);
    DB::writeText(table_name, wb);
    DB::writeText("\"} ", wb);
    DB::writeIntText(std::forward<T>(value), wb);
    DB::writeChar('\n', wb);
}

void writeExternalStream(DB::WriteBuffer & wb, const std::string & prefix, const DB::StorageExternalStream * external_stream)
{
    const auto & storage_id = external_stream->getStorageID();
    String database_name = storage_id.getDatabaseName();
    String table_name = storage_id.getTableName();

    auto external_stream_counter = external_stream->getExternalStreamCounter();
    if (!external_stream_counter)
        return;

    const auto & counters = external_stream_counter->getCounters();

    for (const auto & [key, value] : counters)
    {
        writeExternalStreamMetrics(wb, prefix, key, "counter", database_name, table_name, value);
    }
}

template <typename T>
void writeAlertMetrics(
    DB::WriteBuffer & wb,
    const std::string & prefix,
    const std::string & key,
    const std::string & help_msg,
    const std::string & type,
    const std::string & database,
    const std::string & name,
    T && value)
{
    std::string metric_name = prefix + key;
    writeOutLine(wb, "# HELP", metric_name, help_msg);
    writeOutLine(wb, "# TYPE", metric_name, type);
    DB::writeText(metric_name, wb);
    DB::writeText("{id=\"", wb);
    DB::writeText("\", database=\"", wb);
    DB::writeText(database, wb);
    DB::writeText("\", name=\"", wb);
    DB::writeText(name, wb);
    DB::writeText("\"} ", wb);
    writeOutLine(wb, std::forward<T>(value));
}

void writeAlert(DB::WriteBuffer & wb, const std::string & prefix, const DB::StorageAlert * alert)
{
    const auto & storage_id = alert->getStorageID();
    String database_name = storage_id.getDatabaseName();
    String alert_name = storage_id.getTableName();

    const auto & metrics = alert->getMetrics();

    writeAlertMetrics(
        wb,
        prefix,
        "TriggerCount",
        "Number of times the alert has been triggered",
        "counter",
        database_name,
        alert_name,
        metrics.trigger_count.load());
    writeAlertMetrics(
        wb,
        prefix,
        "ThrottleCount",
        "Number of times the alert has been throttled",
        "counter",
        database_name,
        alert_name,
        metrics.throttle_count.load());
    writeAlertMetrics(
        wb, prefix, "ErrorCount", "Number of times error has happened", "counter", database_name, alert_name, metrics.error_count.load());
}

template <typename T>
void writeMaterializedViewMetrics(
    DB::WriteBuffer & wb,
    const std::string & prefix,
    const std::string & key,
    const std::string & help_msg,
    const std::string & type,
    const std::string & database_name,
    const std::string & table_name,
    const std::string & query_id,
    T && value)
{
    std::string metric_name = prefix + key;
    writeOutLine(wb, "# HELP", metric_name, help_msg);
    writeOutLine(wb, "# TYPE", metric_name, type);
    DB::writeText(metric_name, wb);
    DB::writeText("{database=\"", wb);
    DB::writeText(database_name, wb);
    DB::writeText("\", name=\"", wb);
    DB::writeText(table_name, wb);
    DB::writeText("\", id=\"", wb);
    DB::writeText(query_id, wb);
    DB::writeText("\"} ", wb);
    writeOutLine(wb, std::forward<T>(value));
}

void writeMaterializedView(DB::WriteBuffer & wb, const std::string & prefix, const DB::StorageMaterializedView * materialized_view)
{
    const auto & storage_id = materialized_view->getStorageID();
    String database_name = storage_id.getDatabaseName();
    String table_name = storage_id.getTableName();
    String query_id = materialized_view->getInnerQueryId();

    writeMaterializedViewMetrics(
        wb,
        prefix,
        "QueryStatus",
        "Background query status, 0 - Unknown, 1 - CheckingDependencies, 2 - BuildingPipeline, 3 - ExecutingPipeline, 4 - Error",
        "gauge",
        database_name,
        table_name,
        query_id,
        static_cast<UInt8>(materialized_view->getPipelineStatus()));
    writeMaterializedViewMetrics(
        wb,
        prefix,
        "QueryRecovery",
        "Number of times the background query has recovered",
        "counter",
        database_name,
        table_name,
        query_id,
        materialized_view->getRetryTimes());
    writeMaterializedViewMetrics(
        wb,
        prefix,
        "QueryNode",
        "Node the background query is running on",
        "gauge",
        database_name,
        table_name,
        query_id,
        materialized_view->isExecutingNode() ? 1 : 0);
    writeMaterializedViewMetrics(
        wb,
        prefix,
        "CheckpointDiskUsage",
        "Disk usage of the background query checkpoint",
        "gauge",
        database_name,
        table_name,
        query_id,
        materialized_view->getCheckpointSize());
    writeMaterializedViewMetrics(
        wb,
        prefix,
        "StorageSize",
        "Disk usage of the inner storage of the materialized view",
        "gauge",
        database_name,
        table_name,
        query_id,
        materialized_view->getStorageSize());
}

template <typename T>
void writeForStorage(const DB::ContextPtr & context, std::function<void(const T *)> cb)
{
    auto databases = DB::DatabaseCatalog::instance().getDatabases();

    for (const auto & [_, database] : databases)
    {
        for (auto table_it = database->getTablesIterator(context); table_it->isValid(); table_it->next())
        {
            DB::StoragePtr table_ptr = table_it->table();

            if (auto * storage = table_ptr->as<T>())
            {
                cb(storage);
            }
        }
    }
}

}

namespace DB
{

PrometheusMetricsWriter::PrometheusMetricsWriter(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_name,
    const AsynchronousMetrics & async_metrics_,
    ContextPtr context_)
    : async_metrics(async_metrics_)
    , context(context_)
    , send_events(config.getBool(config_name + ".events", true))
    , send_metrics(config.getBool(config_name + ".metrics", true))
    , send_asynchronous_metrics(config.getBool(config_name + ".asynchronous_metrics", true))
    , send_status_info(config.getBool(config_name + ".status_info", true))
    , send_external_stream(config.getBool(config_name + ".external_stream", true))
    , send_materialized_view(config.getBool(config_name + ".materialized_view", true))
    , send_query_info(config.getBool(config_name + ".query_info", true))
    , send_alert(config.getBool(config_name + ".alert", true))
{
}

void PrometheusMetricsWriter::write(WriteBuffer & wb) const
{
    if (send_events)
    {
        for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);

            std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
            std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(i))};

            if (!replaceInvalidChars(metric_name))
                continue;
            std::string key{profile_events_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "counter");
            writeOutLine(wb, key, counter);
        }
    }

    if (send_metrics)
    {
        for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        {
            const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

            std::string metric_name{CurrentMetrics::getName(static_cast<CurrentMetrics::Metric>(i))};
            std::string metric_doc{CurrentMetrics::getDocumentation(static_cast<CurrentMetrics::Metric>(i))};

            if (!replaceInvalidChars(metric_name))
                continue;
            std::string key{current_metrics_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "gauge");
            writeOutLine(wb, key, value);
        }
    }

    if (send_asynchronous_metrics)
    {
        auto async_metrics_values = async_metrics.getValues();
        for (const auto & name_value : async_metrics_values)
        {
            std::string key{asynchronous_metrics_prefix + name_value.first};

            if (!replaceInvalidChars(key))
                continue;
            auto value = name_value.second;

            // TODO: add HELP section? asynchronous_metrics contains only key and value
            writeOutLine(wb, "# TYPE", key, "gauge");
            writeOutLine(wb, key, value);
        }
    }

    if (send_status_info)
    {
        for (size_t i = 0, end = CurrentStatusInfo::end(); i < end; ++i)
        {
            std::lock_guard<std::mutex> lock(CurrentStatusInfo::locks[static_cast<CurrentStatusInfo::Status>(i)]);
            std::string metric_name{CurrentStatusInfo::getName(static_cast<CurrentStatusInfo::Status>(i))};
            std::string metric_doc{CurrentStatusInfo::getDocumentation(static_cast<CurrentStatusInfo::Status>(i))};

            if (!replaceInvalidChars(metric_name))
                continue;
            std::string key{current_status_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "gauge");

            for (const auto & value : CurrentStatusInfo::values[i])
            {
                for (const auto & enum_value : CurrentStatusInfo::getAllPossibleValues(static_cast<CurrentStatusInfo::Status>(i)))
                {
                    DB::writeText(key, wb);
                    DB::writeChar('{', wb);
                    DB::writeText(key, wb);
                    DB::writeChar('=', wb);
                    writeDoubleQuotedString(enum_value.first, wb);
                    DB::writeText(",name=", wb);
                    writeDoubleQuotedString(value.first, wb);
                    DB::writeText("} ", wb);
                    DB::writeText(value.second == enum_value.second, wb);
                    DB::writeChar('\n', wb);
                }
            }
        }
    }

    if (send_external_stream)
    {
        writeForStorage<StorageExternalStream>(context, [&](const StorageExternalStream * external_stream) {
            writeExternalStream(wb, external_stream_prefix, external_stream);
        });
    }

    if (send_materialized_view)
    {
        writeForStorage<StorageMaterializedView>(context, [&](const StorageMaterializedView * materialized_view) {
            writeMaterializedView(wb, materialized_view_prefix, materialized_view);
        });
    }

    if (send_query_info)
    {
        ProcessList::Info info = context->getProcessList().getInfo(true, true, true);
        for (const auto & process : info)
            writeQueryInfo(wb, query_info_prefix, process);
    }

    if (send_alert)
    {
        writeForStorage<StorageAlert>(context, [&](const StorageAlert * alert) { writeAlert(wb, alert_prefix, alert); });
    }
}
}
