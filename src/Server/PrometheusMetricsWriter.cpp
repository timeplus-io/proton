#include "PrometheusMetricsWriter.h"

#include <algorithm>

#include <IO/WriteHelpers.h>
#include <Common/StatusInfo.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <regex>

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
    /// dirty solution
    metric_name = std::regex_replace(metric_name, std::regex("[^a-zA-Z0-9_:]"), "_");
    metric_name = std::regex_replace(metric_name, std::regex("^[^a-zA-Z]*"), "");
    return !metric_name.empty();
}

}


namespace DB
{

PrometheusMetricsWriter::PrometheusMetricsWriter(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
    const AsynchronousMetrics & async_metrics_, ContextPtr context_)
    : async_metrics(async_metrics_)
    , context(context_)
    , send_events(config.getBool(config_name + ".events", true))
    , send_metrics(config.getBool(config_name + ".metrics", true))
    , send_asynchronous_metrics(config.getBool(config_name + ".asynchronous_metrics", true))
    , send_status_info(config.getBool(config_name + ".status_info", true))
    , send_external_stream(config.getBool(config_name + ".external_stream", true))
{
}

void PrometheusMetricsWriter::write(WriteBuffer & wb) const
{
    if (send_events)
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
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

            for (const auto & value: CurrentStatusInfo::values[i])
            {
                for (const auto & enum_value: CurrentStatusInfo::getAllPossibleValues(static_cast<CurrentStatusInfo::Status>(i)))
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
        auto databases = DatabaseCatalog::instance().getDatabases();

        for (const auto & [_, database] : databases)
        {
            for (auto table_it = database->getTablesIterator(context); table_it->isValid(); table_it->next())
            {
                StoragePtr table_ptr = table_it->table();

                if(auto * external_stream = table_ptr->as<StorageExternalStream>())
                {
                    const auto & storage_id = external_stream->getStorageID();
                    String database_name = storage_id.getDatabaseName();
                    String table_name = storage_id.getTableName();

                    auto external_stream_counter = external_stream->getExternalStreamCounter();
                    if (!external_stream_counter)
                        continue;

                    const auto & counters = external_stream_counter->getCounters();

                    for (const auto & [metric_name, value] : counters)
                    {
                        std::string key{external_stream_prefix + metric_name};
                        writeOutLine(wb, "# TYPE", key, "counter");
                        DB::writeText(key, wb);
                        DB::writeText("{database=\"", wb);
                        DB::writeText(database_name, wb);
                        DB::writeText("\", name=\"", wb);
                        DB::writeText(table_name, wb);
                        DB::writeText("\"} ", wb);
                        DB::writeIntText(value, wb);
                        DB::writeChar('\n', wb);
                    }
                }
            }
        }
    }
}

}
