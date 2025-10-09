#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/TablesLoader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>

#include <Interpreters/MetadataHelper.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric DatabaseOrdinaryThreads;
    extern const Metric DatabaseOrdinaryThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE_ENGINE;
}

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace
{
    void tryAttachTable(
        ContextMutablePtr context,
        const ASTCreateQuery & query,
        DatabaseOrdinary & database,
        const String & database_name,
        const String & metadata_path,
        bool force_restore,
        Int32 schema_version)
    {
        try
        {
            auto [table_name, table] = createTableFromAST(
                query,
                database_name,
                database.getTableDataPath(query),
                context,
                force_restore,
                schema_version);

            /// proton: starts. issue-739, first change the in memory create query to avoid race
            const auto & new_create_query = parseCreateQueryFromAST(&query, database_name, table_name);
            table->setInMemoryCreateQuery(new_create_query);
            /// proton: ends.

            database.attachTable(context, table_name, table, database.getTableDataPath(query));
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach stream " + backQuote(database_name) + "." + backQuote(query.getTable()) + " from metadata file " + metadata_path
                + " from query " + serializeAST(query));
            throw;
        }
    }
}


DatabaseOrdinary::DatabaseOrdinary(const String & name_, const String & metadata_path_, ContextPtr context_)
    : DatabaseOrdinary(name_, metadata_path_, "data/" + escapeForFileName(name_) + "/", "DatabaseOrdinary (" + name_ + ")", context_)
{
}

DatabaseOrdinary::DatabaseOrdinary(
    const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr context_)
    : DatabaseOnDisk(name_, metadata_path_, data_path_, logger, context_)
{
}

void DatabaseOrdinary::loadStoredObjects(
    ContextMutablePtr local_context, bool force_restore, bool force_attach, bool skip_startup_tables)
{
    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */

    ParsedTablesMetadata metadata;
    loadTablesMetadata(local_context, metadata, force_attach);

    size_t total_tables = metadata.parsed_tables.size() - metadata.total_dictionaries;

    AtomicStopwatch watch;
    std::atomic<size_t> dictionaries_processed{0};
    std::atomic<size_t> tables_processed{0};

    ThreadPool pool(CurrentMetrics::DatabaseOrdinaryThreads, CurrentMetrics::DatabaseOrdinaryThreadsActive);

    /// We must attach dictionaries before attaching tables
    /// because while we're attaching tables we may need to have some dictionaries attached
    /// (for example, dictionaries can be used in the default expressions for some tables).
    /// On the other hand we can attach any dictionary (even sourced from ClickHouse table)
    /// without having any tables attached. It is so because attaching of a dictionary means
    /// loading of its config only, it doesn't involve loading the dictionary itself.

    /// Attach dictionaries.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (create_query.isDictionary())
        {
            pool.scheduleOrThrowOnError([&]()
            {
                loadTableFromMetadata(local_context, name, name_with_path_and_query.second, force_restore);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++dictionaries_processed, metadata.total_dictionaries, watch);
            });
        }
    }

    pool.wait();

    /// Attach tables.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (!create_query.isDictionary())
        {
            pool.scheduleOrThrowOnError([&]()
            {
                loadTableFromMetadata(local_context, name, name_with_path_and_query.second, force_restore);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
        }
    }

    pool.wait();

    if (!skip_startup_tables)
    {
        /// After all tables was basically initialized, startup them.
        startupTables(pool, force_restore, force_attach);
    }
}

void DatabaseOrdinary::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto process_metadata = [&metadata, is_startup, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                create_query->setDatabase(database_name);

                /// Even if we don't load the table we can still mark the uuid of it as taken.
                if (create_query->uuid != UUIDHelpers::Nil)
                {
                    /// A bit tricky way to distinguish ATTACH DATABASE and server startup (actually it's "force_attach" flag).
                    if (is_startup)
                    {
                        /// Server is starting up. Lock UUID used by permanently detached table.
                        DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);
                    }
                    else if (!DatabaseCatalog::instance().hasUUIDMapping(create_query->uuid))
                    {
                        /// It's ATTACH DATABASE. UUID for permanently detached table must be already locked.
                        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
                        if (getEngineName() != "MaterializedPostgreSQL")
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create_query->uuid);
                    }
                }

                if (fs::exists(full_path.string() + detached_suffix))
                {
                    const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));
                    LOG_DEBUG(log, "Skipping permanently detached stream {}.", backQuote(table_name));
                    return;
                }

                QualifiedTableName qualified_name{database_name, create_query->getTable()};
                TableNamesSet loading_dependencies = getDependenciesSetFromCreateQuery(getContext(), qualified_name, ast);

                std::lock_guard lock{metadata.mutex};

                /// proton: starts For pure local system tables assign local placements
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast, 1};
                /// proton: ends
                if (loading_dependencies.empty())
                {
                    metadata.independent_database_objects.emplace_back(std::move(qualified_name));
                }
                else
                {
                    for (const auto & dependency : loading_dependencies)
                        metadata.dependencies_info[dependency].dependent_database_objects.insert(qualified_name);
                    assert(metadata.dependencies_info[qualified_name].dependencies.empty());
                    metadata.dependencies_info[qualified_name].dependencies = std::move(loading_dependencies);
                }
                metadata.total_dictionaries += create_query->isDictionary();
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(local_context, process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "local Metadata processed, database {} has {} local tables and {} dictionaries in total.",
             database_name, tables_in_database, dictionaries_in_database);

    /// proton : starts. A database can contain different type of tables
    /// 1) local tables which are private only to that node (for example, system database most of the time contains these kind of tables)
    /// 2) distributed tables which are visible to all nodes (for example, default database most of the time contains these kind of tables)
    ///
    /// The above logic loads the metadata for private tables which are defined in plain text files on file systems
    /// Then following logic loads metadata for distributed tables which are persisted in metastore
    loadTablesMetadataFromMetaStore(local_context, metadata);

    loadAlertsMetadata(local_context, metadata);
    /// proton: ends
}

void DatabaseOrdinary::loadTableFromMetadata(ContextMutablePtr local_context, const QualifiedTableName & name, const ParsedTableMetadata & parsed_meta_data, bool force_restore)
{
    assert(name.database == database_name);

    /// proton: starts
    if (parsed_meta_data.ast == nullptr)
    {
        chassert(alert_descriptors.contains(name.table));
        try
        {
            loadAlert(name, local_context);
        }
        catch (const Exception & ex)
        {
            LOG_ERROR(log, "Failed to load alert {}, error={}", name, ex.message());
        }
        return;
    }
    /// proton: ends

    const auto & create_query = parsed_meta_data.ast->as<const ASTCreateQuery &>();

    try
    {
        tryAttachTable(
            local_context,
            create_query,
            *this,
            name.database,
            parsed_meta_data.path,
            force_restore,
            parsed_meta_data.schema_version);
    }
    catch (const Exception & ex)
    {
        LOG_INFO(log, "Failed to load table, error={} ddl={}", ex.message(), queryToString(create_query, true));
    }
}

void DatabaseOrdinary::startupTables(ThreadPool & thread_pool, bool /*force_restore*/, bool /*force_attach*/)
{
    LOG_INFO(log, "Starting up tables.");

    const size_t total_tables = tables.size();
    if (!total_tables)
        return;

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto startup_one_table = [&](const StoragePtr & table)
    {
        table->startup();
        logAboutProgress(log, ++tables_processed, total_tables, watch);
    };


    try
    {
        for (const auto & table : tables)
            thread_pool.scheduleOrThrowOnError([&]() { startup_one_table(table.second); });
    }
    catch (...)
    {
        /// We have to wait for jobs to finish here, because job function has reference to variables on the stack of current thread.
        thread_pool.wait();
        throw;
    }
    thread_pool.wait();
}

void DatabaseOrdinary::alterTable(
    ContextPtr local_context,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    String /*alter_command*/,
    std::vector<String> /*alter_command_ast*/)
{
    String table_name = table_id.table_name;
    /// Read the definition of the table and replace the necessary parts with new ones.
    String table_metadata_path;
    String table_metadata_tmp_path;
    String statement;

    StoragePtr table = tryGetTable(table_name, local_context);
    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_STREAM, "Stream {}.{} doesn't exist", backQuote(database_name), backQuote(table_name));

    table_metadata_path = getObjectMetadataPath(table_name);
    {
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        statement.data(),
        statement.data() + statement.size(),
        "in file " + table_metadata_path,
        0,
        local_context->getSettingsRef().max_parser_depth);

    applyMetadataChangesToCreateQuery(ast, metadata);

    statement = getObjectDefinitionFromCreateQuery(ast);

    table_metadata_tmp_path = table_metadata_path + ".tmp";
    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (local_context->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    TableNamesSet new_dependencies = getDependenciesSetFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    DatabaseCatalog::instance().updateLoadingDependencies(table_id, std::move(new_dependencies));

    commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, local_context);

    /// proton: starts.
    const auto & new_create_query = parseCreateQueryFromAST(ast, database_name, table_id.table_name);
    table->setInMemoryCreateQuery(new_create_query);
    /// proton: ends.
}

void DatabaseOrdinary::commitAlterTable(const StorageID &, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & /*statement*/, ContextPtr /*query_context*/)
{
    try
    {
        /// rename atomically replaces the old file with the new one.
        fs::rename(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        throw;
    }
}

void registerDatabaseOrdinary(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (!args.create_query.attach && !args.context->getSettingsRef().allow_deprecated_database_ordinary)
            throw Exception(
                ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                "Ordinary database engine is deprecated (see also allow_deprecated_database_ordinary setting)");
        return make_shared<DatabaseOrdinary>(
            args.database_name,
            args.metadata_path,
            args.context);
    };
    factory.registerDatabase("Ordinary", create_fn);
}
}
