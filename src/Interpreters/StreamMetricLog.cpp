#include <Interpreters/StreamMetricLog.h>

#include <Cluster/Common/TimeWheel/TimerService.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>
#include <Common/timeScale.h>


namespace DB
{
namespace
{
bool ignoreDatabase(const String & name)
{
    static const std::unordered_set<std::string> black_list
        = {DatabaseCatalog::TEMPORARY_DATABASE,
           DatabaseCatalog::SYSTEM_DATABASE,
           DatabaseCatalog::INFORMATION_SCHEMA,
           DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE};

    return black_list.contains(name);
}
}

namespace ErrorCodes
{
extern const int CANNOT_CREATE_TIMER;
}

NamesAndTypesList StreamMetricLogElement::getNamesAndTypes()
{
    return {
        {"elapsed_ms", std::make_shared<DataTypeInt64>()},
        {"node_id", std::make_shared<DataTypeUInt64>()},
        {"database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"type", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeFixedString>(32))},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"written_bytes", std::make_shared<DataTypeUInt64>()},
        {"written_rows", std::make_shared<DataTypeUInt64>()},
        {"external_ingress", DataTypeFactory::instance().get(TypeIndex::Bool)},
        {"_tp_time", std::make_shared<DataTypeDateTime64>(3)},
        {"_tp_sn", std::make_shared<DataTypeInt64>()},
    };
}

void StreamMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(elapsed_ms);

    columns[column_idx++]->insert(node_id);
    columns[column_idx++]->insert(database);
    columns[column_idx++]->insert(stream_name);
    columns[column_idx++]->insert(uuid);
    columns[column_idx++]->insert(type);

    columns[column_idx++]->insert(read_bytes);
    columns[column_idx++]->insert(read_rows);
    columns[column_idx++]->insert(written_bytes);
    columns[column_idx++]->insert(written_rows);
    columns[column_idx++]->insert(external_ingress);
    columns[column_idx++]->insert(_tp_time);
}

StreamMetricLog::StreamMetricLog(
    ContextPtr context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : SystemLog<StreamMetricLogElement>(context_, database_name_, table_name_, storage_def_, flush_interval_milliseconds_)
{
    is_force_prepare_tables = true;
}

void StreamMetricLog::startCollectMetrics(int64_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;

    timer_task = getContext()->getTimerService().add(collect_interval_milliseconds, [this]() { return collectMetrics(); }, /*repeat=*/true);
    if (!timer_task)
        throw Exception(ErrorCodes::CANNOT_CREATE_TIMER, "Failed to add collect metrics task to timer");
}

void StreamMetricLog::stopCollectMetrics()
{
    if (stopped.test_and_set())
        return;

    if (timer_task)
        timer_task->cancel();
}

void StreamMetricLog::shutdown()
{
    stopCollectMetrics();
    stopFlushThread();
}

void StreamMetricLog::collectMetrics()
{
    try
    {
        setThreadName("SMCollector");
        doCollectMetrics();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to collect stream metrics");
    }
}

void StreamMetricLog::doCollectMetrics()
{
    /// List all storages [in the specified database]
    Databases databases = DatabaseCatalog::instance().getDatabases();
    auto context = getContext();
    auto node_id = context->getNodeID();

    for (const auto & [database_name, database] : databases)
    {
        if (ignoreDatabase(database_name))
            continue;

        if (!database->canContainMergeTreeTables())
            continue;

        for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & storage = iterator->table();
            if (!storage)
                continue;

            const auto & storage_id = storage->getStorageID();

            StreamMetricLogElement elem{};
            elem._tp_time = nowSubsecond(3);

            {
                const int64_t now_ms = MonotonicMilliseconds::now();
                const int64_t last_metric_time = storage->getMetricTime(/*reset=*/true);
                elem.elapsed_ms = (last_metric_time > 0 && last_metric_time <= now_ms) ? (now_ms - last_metric_time) : 0;
            }

            elem.node_id = node_id;
            elem.database = storage_id.getDatabaseName();
            elem.stream_name = storage_id.getTableName();
            elem.uuid = storage_id.uuid;
            elem.type = storage->getName();

            elem.read_bytes = storage->readBytes(/*reset=*/true, /*external_ingress*/ false);
            elem.read_rows = storage->readRows(/*reset=*/true, /*external_ingress*/ false);
            elem.written_bytes = storage->writtenBytes(/*reset=*/true, /*external_ingress*/ false);
            elem.written_rows = storage->writtenRows(/*reset=*/true, /*external_ingress*/ false);

            if (auto * /*storage_external_stream*/ _ = dynamic_cast<StorageExternalStream *>(storage.get()))
            {
                elem.external_ingress = true;
            }
            else if (auto * /*storage_external_table*/ _ = dynamic_cast<StorageExternalTable *>(storage.get()))
            {
                elem.external_ingress = true;
            }
            else if (auto * storage_stream = dynamic_cast<StorageStream *>(storage.get()))
            {
                elem.external_ingress = false;
                this->add(elem);

                elem.read_bytes = storage_stream->readBytes(/*reset=*/true, /*external_ingress*/ true);
                elem.read_rows = storage_stream->readRows(/*reset=*/true, /*external_ingress*/ true);
                elem.written_bytes = storage_stream->writtenBytes(/*reset=*/true, /*external_ingress*/ true);
                elem.written_rows = storage_stream->writtenRows(/*reset=*/true, /*external_ingress*/ true);
                elem.external_ingress = true;
            }
            else if (auto * storage_materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
            {
                if (!storage_materialized_view->getExternalTargetTableID())
                {
                    if (const auto & inner_storage = storage_materialized_view->tryGetTargetTable())
                    {
                        StreamMetricLogElement inner_storage_elem{};

                        inner_storage_elem._tp_time = nowSubsecond(3);
                        inner_storage_elem.elapsed_ms = MonotonicMilliseconds::now() - inner_storage->getMetricTime();
                        inner_storage_elem.node_id = node_id;
                        const auto & inner_storage_id = inner_storage->getStorageID();
                        inner_storage_elem.database = inner_storage_id.getDatabaseName();
                        inner_storage_elem.stream_name = inner_storage_id.getTableName();
                        inner_storage_elem.uuid = inner_storage_id.uuid;
                        inner_storage_elem.type = inner_storage->getName();

                        inner_storage_elem.read_bytes = inner_storage->readBytes();
                        inner_storage_elem.read_rows = inner_storage->readRows();
                        inner_storage_elem.written_bytes = inner_storage->writtenBytes();
                        inner_storage_elem.written_rows = inner_storage->writtenRows();

                        if (auto * inner_storage_stream = dynamic_cast<StorageStream *>(inner_storage.get()))
                        {
                            elem.external_ingress = false;
                            this->add(elem);

                            elem.read_bytes = inner_storage_stream->readBytes(/*reset=*/true, /*external_ingress*/ true);
                            elem.read_rows = inner_storage_stream->readRows(/*reset=*/true, /*external_ingress*/ true);
                            elem.written_bytes = inner_storage_stream->writtenBytes(/*reset=*/true, /*external_ingress*/ true);
                            elem.written_rows = inner_storage_stream->writtenRows(/*reset=*/true, /*external_ingress*/ true);
                            elem.external_ingress = true;
                        }

                        this->add(inner_storage_elem);
                    }
                }
            }

            this->add(elem);
        }
    }
}
}
