#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetadataHelper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ProtonCommon.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Disks/IStoragePolicy.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypeUUID.h>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Cluster/Protocol/ListStreamsResponseData.h>
#include <Storages/MatView/StorageMaterializedView.h>
/// proton: ends


namespace DB
{


StorageSystemTables::StorageSystemTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"is_temporary", std::make_shared<DataTypeUInt8>()},
        {"data_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"metadata_modification_time", std::make_shared<DataTypeDateTime>()},
        {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"create_table_query", std::make_shared<DataTypeString>()},
        {"engine_full", std::make_shared<DataTypeString>()},
        {"mode", std::make_shared<DataTypeString>()},
        {"as_select", std::make_shared<DataTypeString>()},
        {"partition_key", std::make_shared<DataTypeString>()},
        {"sorting_key", std::make_shared<DataTypeString>()},
        {"primary_key", std::make_shared<DataTypeString>()},
        {"sampling_key", std::make_shared<DataTypeString>()},
        {"storage_policy", std::make_shared<DataTypeString>()},
        {"total_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"total_bytes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"lifetime_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"lifetime_bytes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"comment", std::make_shared<DataTypeString>()},
        {"has_own_data", std::make_shared<DataTypeUInt8>()},
        {"loading_dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"loading_dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"loading_dependent_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"loading_dependent_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        /// proton : starts
        {"schema_version", std::make_shared<DataTypeInt32>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"written_bytes", std::make_shared<DataTypeUInt64>()},
        {"written_rows", std::make_shared<DataTypeUInt64>()},
        /// proton : ends
    }, {
        {"table", std::make_shared<DataTypeString>(), "name"}
    }));
    setInMemoryMetadata(storage_metadata);
}


namespace
{

ColumnPtr getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & database_name : databases | boost::adaptors::map_keys)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        column->insert(database_name);
    }

    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

ColumnPtr getFilteredTables(const ActionsDAG::Node * predicate, const ColumnPtr & filtered_databases_column, ContextPtr context)
{
    Block sample {
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "engine")
    };

    MutableColumnPtr database_column = ColumnString::create();
    MutableColumnPtr engine_column;

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample);
    if (dag)
    {
        bool filter_by_engine = false;
        for (const auto * input : dag->getInputs())
            if (input->result_name == "engine")
                filter_by_engine = true;

        if (filter_by_engine)
            engine_column= ColumnString::create();
    }

    for (size_t database_idx = 0; database_idx < filtered_databases_column->size(); ++database_idx)
    {
        const auto & database_name = filtered_databases_column->getDataAt(database_idx).toString();
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
            continue;

        for (auto table_it = database->getTablesIterator(context); table_it->isValid(); table_it->next())
        {
            database_column->insert(table_it->name());
            if (engine_column)
            {
                if (const auto & storage = table_it->table(); storage)
                    engine_column->insert(table_it->table()->getName());
                else
                    engine_column->insert("Unknown");
            }
        }
    }

    Block block {ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "name")};
    if (engine_column)
        block.insert(ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine"));

    if (dag)
        VirtualColumnUtils::filterBlockWithDAG(dag, block, context);

    return block.getByPosition(0).column;
}

/// Avoid heavy operation on tables if we only queried columns that we can get without table object.
/// Otherwise it will require table initialization for Lazy database.
static bool needTable(const DatabasePtr & database, const Block & header)
{
    if (database->getEngineName() != "Lazy")
        return true;

    static const std::set<std::string> columns_without_table = { "database", "name", "uuid", "metadata_modification_time" };
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        if (!columns_without_table.contains(column.name))
            return true;
    }
    return false;
}


class TablesBlockSource final : public ISource
{
public:
    TablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        ContextPtr context_)
        : ISource(std::move(header), true, ProcessorID::TablesBlockSourceID)
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
    {
        size_t size = tables_->size();
        tables.reserve(size);
        for (size_t idx = 0; idx < size; ++idx)
            tables.insert(tables_->getDataAt(idx).toString());

        auto & metastore = Globals::getMetaStore();
        auto list_stream_resp = metastore.listStreams(std::make_shared<cluster::ListStreamsRequest>(
            "", "", metastore.nodeID(), /*consistent_read_=*/false, /*timeout_ms=*/10'000, /*request_version=*/2));
        if (list_stream_resp->hasError())
        {
            const auto & err = list_stream_resp->error();
            throw Exception(err.error_code, "Failed to load all streams error_message={}", err.error_message);
        }

        stream_descs.reserve(list_stream_resp->data().stream_descs.size());
        for (const auto & stream_desc : list_stream_resp->data().stream_descs)
            stream_descs[stream_desc->stream.id] = stream_desc;
        /// proton: ends.
    }

    String getName() const override { return "Tables"; }

protected:
    Chunk generate() override
    {
        if (done)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (!database)
                {
                    /// Database was deleted just now or the user has no access.
                    ++database_idx;
                    continue;
                }

                break;
            }

            /// This is for temporary tables. They are output in single block regardless to max_block_size.
            if (database_idx >= databases->size())
            {
                if (context->hasSessionContext())
                {
                    Tables external_tables = context->getSessionContext()->getExternalTables();

                    for (auto & table : external_tables)
                    {
                        size_t src_index = 0;
                        size_t res_index = 0;

                        // database
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // name
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.first);

                        // uuid
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getStorageID().uuid);

                        // engine
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        // is_temporary
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(1u);

                        // data_paths
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // metadata_path
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // metadata_modification_time
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // dependencies_database
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // dependencies_table
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // create_table_query
                        if (columns_mask[src_index++])
                        {
                            auto temp_db = DatabaseCatalog::instance().getDatabaseForTemporaryTables();
                            ASTPtr ast = temp_db ? temp_db->tryGetCreateTableQuery(table.second->getStorageID().getTableName(), context) : nullptr;
                            res_columns[res_index++]->insert(ast ? ast->formatForLogging() : "");
                        }

                        // engine_full
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        // mode
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        /// Fill the rest columns with defaults
                        while (src_index < columns_mask.size())
                            if (columns_mask[src_index++])
                                res_columns[res_index++]->insertDefault();
                    }
                }

                UInt64 num_rows = res_columns.at(0)->size();
                done = true;
                return Chunk(std::move(res_columns), num_rows);
            }

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context);

            const bool need_table = needTable(database, getPort().getHeader());

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (!tables.contains(table_name))
                    continue;

                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                StoragePtr table = nullptr;
                TableLockHolder lock;
                if (need_table)
                {
                    table = tables_it->table();
                    if (!table)
                        // Table might have just been removed or detached for Lazy engine (see DatabaseLazy::tryGetTable())
                        continue;

                    /// The only column that requires us to hold a shared lock is data_paths as rename might alter them (on ordinary tables)
                    /// and it's not protected internally by other mutexes
                    static const size_t DATA_PATHS_INDEX = 5;
                    if (columns_mask[DATA_PATHS_INDEX])
                    {
                        lock = table->tryLockForShare(context->getCurrentQueryId(),
                                                      context->getSettingsRef().lock_acquire_timeout);
                        if (!lock)
                            // Table was dropped while acquiring the lock, skipping table
                            continue;
                    }
                }
                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);

                /// proton: starts.
                auto uuid = tables_it->uuid();
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(uuid);
                /// proton: ends.

                if (columns_mask[src_index++])
                {
                    chassert(table != nullptr);
                    res_columns[res_index++]->insert(table->getName());
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(0u);  // is_temporary

                if (columns_mask[src_index++])
                {
                    chassert(lock != nullptr);
                    Array table_paths_array;
                    auto paths = table->getDataPaths();
                    table_paths_array.reserve(paths.size());
                    for (const String & path : paths)
                        table_paths_array.push_back(path);
                    res_columns[res_index++]->insert(table_paths_array);
                    /// We don't need the lock anymore
                    lock = nullptr;
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database->getObjectMetadataPath(table_name));

                if (columns_mask[src_index++])
                {
                    /// proton: starts.
                    if (uuid != UUIDHelpers::Nil)
                    {
                        if (auto it = stream_descs.find(uuid); it != stream_descs.end())
                            res_columns[res_index++]->insert(static_cast<UInt64>(it->second->last_modify_timestamp_ms / 1000));
                        else
                            res_columns[res_index++]->insert(static_cast<UInt64>(database->getObjectMetadataModificationTime(table_name)));
                    }
                    else
                    {
                        res_columns[res_index++]->insert(static_cast<UInt64>(database->getObjectMetadataModificationTime(table_name)));
                    }
                    /// proton: ends.
                }

                {
                    Array dependencies_table_name_array;
                    Array dependencies_database_name_array;
                    if (columns_mask[src_index] || columns_mask[src_index + 1])
                    {
                        const auto dependencies = DatabaseCatalog::instance().getDependencies(StorageID(database_name, table_name));

                        dependencies_table_name_array.reserve(dependencies.size());
                        dependencies_database_name_array.reserve(dependencies.size());
                        for (const auto & dependency : dependencies)
                        {
                            dependencies_table_name_array.push_back(dependency.table_name);
                            dependencies_database_name_array.push_back(dependency.database_name);
                        }
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_database_name_array);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_table_name_array);
                }

                if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2] || columns_mask[src_index + 3])
                {
                    /// proton: starts.
                    /// getInMemoryCreateQuery() requires explicit setInMemoryCreateQuery() call during table creation.
                    /// but we didn't apply this same logic for MysqlDB/PostgreSQL/Iceberg previously.
                    StorageInMemoryCreateQueryPtr create_query_snapshot;
                    assert(table);
                    create_query_snapshot = table->getInMemoryCreateQuery();

                    if (create_query_snapshot)
                    {
                        // create_table_query
                        if (columns_mask[src_index++])
                        {
                            if (context->getSettingsRef().show_uuid)
                                res_columns[res_index++]->insert(create_query_snapshot->getQueryUUID());
                            else
                                res_columns[res_index++]->insert(create_query_snapshot->getQuery());
                        }

                        // engine_full
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(create_query_snapshot->getEngineFull());

                        // mode
                        if (columns_mask[src_index++])
                        {
                            String mode = create_query_snapshot->getMode();
                            if (mode.empty() && table->getName() == "Stream")
                                mode = ProtonConsts::APPEND_MODE;

                            res_columns[res_index++]->insert(mode);
                        }
                    }
                    /// proton: ends.
                    else
                    {
                        ASTPtr ast = database->tryGetCreateTableQuery(table_name, context);
                        auto * ast_create = ast ? ast->as<ASTCreateQuery>() : nullptr;

                        if (ast_create && !context->getSettingsRef().show_uuid)
                        {
                            ast_create->uuid = UUIDHelpers::Nil;
                            ast_create->to_inner_uuid = UUIDHelpers::Nil;
                        }

                        // create_table_query
                        if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(ast ? ast->formatForLogging() : "");

                        // engine_full
                        if (columns_mask[src_index++])
                        {
                            String engine_full;

                            if (ast_create && ast_create->storage)
                            {
                            engine_full = ast_create->formatForLogging();

                                static const char * const extra_head = " ENGINE = ";
                                if (startsWith(engine_full, extra_head))
                                    engine_full = engine_full.substr(strlen(extra_head));
                            }

                            res_columns[res_index++]->insert(engine_full);
                        }

                        // mode
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }

                    // as_select
                    if (columns_mask[src_index++])
                    {
                        /// proton: FIXME: XXX
                        String as_select;
//                        if (ast_create && ast_create->select)
//                            as_select = format({context, *ast_create->select});
                        res_columns[res_index++]->insert(as_select);
                    }
                }
                else
                    src_index += 4;

                StorageMetadataPtr metadata_snapshot;
                if (table)
                    metadata_snapshot = table->getInMemoryMetadataPtr();

                ASTPtr expression_ptr;
                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPartitionKeyAST()))
                        res_columns[res_index++]->insert(expression_ptr->formatForLogging());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSortingKey().expression_list_ast))
                        res_columns[res_index++]->insert(expression_ptr->formatForLogging());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPrimaryKey().expression_list_ast))
                        res_columns[res_index++]->insert(expression_ptr->formatForLogging());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSamplingKeyAST()))
                        res_columns[res_index++]->insert(expression_ptr->formatForLogging());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto policy = table ? table->getStoragePolicy() : nullptr;
                    if (policy)
                        res_columns[res_index++]->insert(policy->getName());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                auto settings = context->getSettingsRef();
                settings.select_sequential_consistency = 0;
                if (columns_mask[src_index++])
                {
                    /// proton: starts
                    std::optional<UInt64> total_rows = std::nullopt;
                    if (table)
                    {
                        if (auto mv = std::dynamic_pointer_cast<StorageMaterializedView>(table); mv)
                        {
                            /// For materialized view, only count the internal stream rows
                            if (!mv->getExternalTargetTableID().has_value())
                                total_rows = mv->getTargetTable()->totalRows(settings);
                        }
                        else
                        {
                            total_rows = table->totalRows(settings);
                        }
                    }
                    /// proton: ends
                    if (total_rows)
                        res_columns[res_index++]->insert(*total_rows);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto total_bytes = table->totalBytes(settings);
                    if (total_bytes)
                        res_columns[res_index++]->insert(*total_bytes);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto lifetime_rows = table ? table->lifetimeRows() : std::nullopt;
                    if (lifetime_rows)
                        res_columns[res_index++]->insert(*lifetime_rows);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto lifetime_bytes = table ? table->lifetimeBytes() : std::nullopt;
                    if (lifetime_bytes)
                        res_columns[res_index++]->insert(*lifetime_bytes);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot)
                        res_columns[res_index++]->insert(metadata_snapshot->comment);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (table)
                        res_columns[res_index++]->insert(table->storesDataOnDisk());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2] || columns_mask[src_index + 3])
                {
                    DependenciesInfo info = DatabaseCatalog::instance().getLoadingDependenciesInfo({database_name, table_name});

                    Array loading_dependencies_databases;
                    Array loading_dependencies_tables;
                    loading_dependencies_databases.reserve(info.dependencies.size());
                    loading_dependencies_tables.reserve(info.dependencies.size());
                    for (auto && dependency : info.dependencies)
                    {
                        loading_dependencies_databases.push_back(dependency.database);
                        loading_dependencies_tables.push_back(dependency.table);
                    }

                    Array loading_dependent_databases;
                    Array loading_dependent_tables;
                    loading_dependent_databases.reserve(info.dependencies.size());
                    loading_dependent_tables.reserve(info.dependencies.size());
                    for (auto && dependent : info.dependent_database_objects)
                    {
                        loading_dependent_databases.push_back(dependent.database);
                        loading_dependent_tables.push_back(dependent.table);
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(loading_dependencies_databases);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(loading_dependencies_tables);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(loading_dependent_databases);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(loading_dependent_tables);
                }
                /// proton : starts. schema_version
                else
                    src_index += 4;

                if (columns_mask[src_index++])
                {
                    assert(table);
                    res_columns[res_index++]->insert(table->schemaVersion());
                }

                // read_bytes
                if (columns_mask[src_index++])
                {
                    assert(table);
                    res_columns[res_index++]->insert(table->readBytes(/*reset=*/false));
                }

                // read_rows
                if (columns_mask[src_index++])
                {
                    assert(table);
                    res_columns[res_index++]->insert(table->readRows(/*reset=*/false));
                }

                // write_bytes
                if (columns_mask[src_index++])
                {
                    assert(table);
                    res_columns[res_index++]->insert(table->writtenBytes(/*reset=*/false));
                }

                // write_rows
                if (columns_mask[src_index++])
                {
                    assert(table);
                    res_columns[res_index++]->insert(table->writtenRows(/*reset=*/false));
                }
                /// proton : ends
            }
        }

        UInt64 num_rows = res_columns.at(0)->size();
        return Chunk(std::move(res_columns), num_rows);
    }
private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    NameSet tables;
    size_t database_idx = 0;
    DatabaseTablesIteratorPtr tables_it;
    ContextPtr context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;
    /// proton: starts.
    /// Stream descriptors loaded from metastore. for external database like MySQL/PostgreSQL/Iceberg the uuid is Null.
    std::unordered_map<UUID, cluster::protocol::StreamDescriptorPtr> stream_descs;
    /// proton: ends
};

}

class ReadFromSystemTables : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemTables"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemTables(
        Block sample_block,
        ContextPtr context_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)})
        , context(std::move(context_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

private:
    ContextPtr context;
    std::vector<UInt8> columns_mask;
    size_t max_block_size;
};

void StorageSystemTables::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    /// Create a mask of what columns are needed in the result.
    NameSet names_set(column_names.begin(), column_names.end());

    Block res_block;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            res_block.insert(sample_block.getByPosition(i));
        }
    }

    auto reading = std::make_unique<ReadFromSystemTables>(
        std::move(res_block),
        context,
        std::move(columns_mask),
        max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemTables::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    ColumnPtr filtered_databases_column = getFilteredDatabases(predicate, context);
    ColumnPtr filtered_tables_column = getFilteredTables(predicate, filtered_databases_column, context);

    Pipe pipe(std::make_shared<TablesBlockSource>(
        std::move(columns_mask), getOutputStream().header, max_block_size, std::move(filtered_databases_column), std::move(filtered_tables_column), context));
    pipeline.init(std::move(pipe));
}

}
