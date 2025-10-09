#include <Common/quoteString.h>
#include <Common/ThreadPool.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/TablesLoader.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>

#include <filesystem>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Interpreters/executeSelectQuery.h>
/// proton: ends


namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric StartupSystemTablesThreads;
    extern const Metric StartupSystemTablesThreadsActive;
}

namespace DB
{

static void executeCreateQuery(
    const String & query,
    ContextMutablePtr context,
    const String & database,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser, query.data(), query.data() + query.size(), "in file " + file_name, 0, context->getSettingsRef().max_parser_depth);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.setDatabase(database);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceAttach(true);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.setLoadDatabaseWithoutTables(true);
    interpreter.execute();
}

static bool isSystemOrInformationSchema(const String & database_name)
{
    return database_name == DatabaseCatalog::SYSTEM_DATABASE ||
           database_name == DatabaseCatalog::INFORMATION_SCHEMA ||
           database_name == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE;
}

static void loadDatabase(
    ContextMutablePtr context,
    const String & database,
    const String & database_path,
    bool force_restore_data,
    bool is_local,
    std::optional<std::string> query = std::nullopt) /// proton : add is_local and query
{
    String database_attach_query;
    String database_metadata_file = database_path + ".sql";

    if (fs::exists(fs::path(database_metadata_file)))
    {
        /// There is .sql file with database creation statement.
        ReadBufferFromFile in(database_metadata_file, 1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else
    {
        /// It's first server run and we need create default and system databases.
        /// .sql file with database engine will be written for CREATE query.
        database_attach_query
            = query.value_or(fmt::format("CREATE DATABASE {} ENGINE = Atomic SETTINGS local={}", backQuoteIfNeed(database), is_local));
    }

    try
    {
        executeCreateQuery(database_attach_query, context, database, database_metadata_file, force_restore_data);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while loading database {} from path {}", backQuote(database), database_path));
        throw;
    }
}


void loadMetadata(ContextMutablePtr context, const std::vector<String> & builtin_databases)
{
    LoggerPtr log = getLogger("loadMetadata");

    String path = context->getPath() + "metadata";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    auto force_restore_data_flag_file = fs::path(context->getFlagsPath()) / "force_restore_data";
    bool has_force_restore_data_flag = fs::exists(force_restore_data_flag_file);

    /// Loop over databases.
    std::map<String, String> databases;
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_symlink())
            continue;

        const auto current_file = it->path().filename().string();
        if (!it->is_directory())
        {
            /// TODO: DETACH DATABASE PERMANENTLY ?
            if (fs::path(current_file).extension() == ".sql")
            {
                String db_name = fs::path(current_file).stem();
                if (!isSystemOrInformationSchema(db_name))
                    databases.emplace(unescapeForFileName(db_name), fs::path(path) / db_name);
            }

            /// Temporary fails may be left from previous server runs.
            if (fs::path(current_file).extension() == ".tmp")
            {
                LOG_WARNING(log, "Removing temporary file {}", it->path().string());
                try
                {
                    fs::remove(it->path());
                }
                catch (...)
                {
                    /// It does not prevent server to startup.
                    tryLogCurrentException(log);
                }
            }

            continue;
        }

        /// For '.svn', '.gitignore' directory and similar.
        if (current_file.at(0) == '.')
            continue;

        if (isSystemOrInformationSchema(current_file))
            continue;

        databases.emplace(unescapeForFileName(current_file), it->path().string());
    }

    /// clickhouse-local creates DatabaseMemory as default database by itself
    /// For clickhouse-server we need create default database
    /// proton: starts. Add `neutron` etc builtin databases. All builtin databases are local provisioned
    for (const auto & builtin_db : builtin_databases)
    {
        bool create_default_db_if_not_exists = !builtin_db.empty();
        bool metadata_dir_for_default_db_already_exists = databases.contains(builtin_db);
        if (create_default_db_if_not_exists && !metadata_dir_for_default_db_already_exists)
            databases.emplace(builtin_db, std::filesystem::path(path) / escapeForFileName(builtin_db));
    }
    /// proton: ends

    TablesLoader::Databases loaded_databases;

    /// proton: starts. Check databases stored in metaDB. Create the database if it is not found in the directory.
    auto maybe_databases = Globals::getMetaStore().getMetaDB().listDatabases();
    if (!maybe_databases.hasError())
    {
        for (const auto & database_desc : maybe_databases.result)
        {
            /// If we find database in metadata store, it has higher precedence to load
            databases.erase(database_desc->name);
            {
                /// Set query_kind to run create query only on local node.
                auto run_local_ctx = Context::createCopy(context);
                run_local_ctx->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
                loadDatabase(
                    run_local_ctx,
                    database_desc->name,
                    std::filesystem::path(path) / escapeForFileName(database_desc->name),
                    has_force_restore_data_flag,
                    true,
                    database_desc->sql_def);
                loaded_databases.insert({database_desc->name, DatabaseCatalog::instance().getDatabase(database_desc->name)});
            }
        }
    }
    /// proton: ends

    for (const auto & [name, db_path] : databases)
    {
        /// proton : starts
        bool is_local = std::find(builtin_databases.begin(), builtin_databases.end(), name) != builtin_databases.end();
        if (!is_local)
        {
            /// Non local databases shall be already loaded from metastore above, but there is a backward compatible issue introduced
            /// between 2.7.x and 2.8.x for single instance: the database was committed locally on file system in plain text file but
            /// not in metastore in 2.7, and in 2.8, we did both. When upgrading from 2.7 to 2.8, we like to just attach the database
            /// without recreating it since recreating it will fail.
            /// ref https://github.com/timeplus-io/proton-enterprise/issues/9793
            LOG_INFO(log, "Found non-local database={} to load. Force local", name);

            auto run_local_ctx = Context::createCopy(context);
            run_local_ctx->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
            loadDatabase(run_local_ctx, name, db_path, has_force_restore_data_flag, /*is_local=*/true);
        }
        else
        {
            loadDatabase(context, name, db_path, has_force_restore_data_flag, /*is_local=*/true);
        }
        /// proton : ends

        loaded_databases.insert({name, DatabaseCatalog::instance().getDatabase(name)});
    }

    TablesLoader loader{context, std::move(loaded_databases), has_force_restore_data_flag, /* force_attach */ true};
    loader.loadTables();
    loader.startupTables();

    if (has_force_restore_data_flag)
    {
        try
        {
            fs::remove(force_restore_data_flag_file);
        }
        catch (...)
        {
            tryLogCurrentException("Load metadata", "Can't remove force restore file to enable data sanity checks");
        }
    }
}

static void loadSystemDatabaseImpl(ContextMutablePtr context, const String & database_name, const String & default_engine)
{
    String path = context->getPath() + "metadata/" + database_name;
    String metadata_file = path + ".sql";
    if (fs::exists(fs::path(path)) || fs::exists(fs::path(metadata_file)))
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, database_name, path, true, true);
    }
    else
    {
        /// Initialize system database manually
        String database_create_query = "CREATE DATABASE ";
        database_create_query += database_name;
        database_create_query += " ENGINE=";
        database_create_query += default_engine;

        /// proton : starts. Add `local=1` which indicates it is private local metadata
        database_create_query += " SETTINGS local=1";
        /// proton : ends

        executeCreateQuery(database_create_query, context, database_name, "<no file>", true);
    }
}


void startupSystemTables()
{
    ThreadPool pool(CurrentMetrics::StartupSystemTablesThreads, CurrentMetrics::StartupSystemTablesThreadsActive);
    DatabaseCatalog::instance().getSystemDatabase()->startupTables(pool, /* force_restore */ true, /* force_attach */ true);
}

void loadMetadataSystem(ContextMutablePtr context)
{
    loadSystemDatabaseImpl(context, DatabaseCatalog::SYSTEM_DATABASE, "Atomic");
    loadSystemDatabaseImpl(context, DatabaseCatalog::INFORMATION_SCHEMA, "Memory");
    loadSystemDatabaseImpl(context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, "Memory");

    TablesLoader::Databases databases =
    {
        {DatabaseCatalog::SYSTEM_DATABASE, DatabaseCatalog::instance().getSystemDatabase()},
        {DatabaseCatalog::INFORMATION_SCHEMA, DatabaseCatalog::instance().getDatabase(DatabaseCatalog::INFORMATION_SCHEMA)},
        {DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, DatabaseCatalog::instance().getDatabase(DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE)},
    };
    TablesLoader loader{context, databases, /* force_restore */ true, /* force_attach */ true};
    loader.loadTables();
    /// Will startup tables in system database after all databases are loaded.
}

}
