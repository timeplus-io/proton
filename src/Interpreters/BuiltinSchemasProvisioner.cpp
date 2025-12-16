#include <Interpreters/BuiltinSchemasProvisioner.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Core/Defines.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/getResource.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>
#include <memory>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

std::string resolveLogPath(const Poco::Util::AbstractConfiguration & config, std::string_view config_key)
{
    namespace fs = std::filesystem;

    const std::string configured = config.getString(std::string{config_key}, "");
    if (configured.empty())
        return {};

    auto exists = [](const std::string & p) { return !p.empty() && fs::exists(fs::path{p}); };

    if (exists(configured))
        return configured;

    /// Fallbacks for embedded.xml based local/dev bootstraps.
    std::vector<std::string> candidates;
    candidates.reserve(8);

    if (config_key == "logger.log")
    {
        candidates.emplace_back("./proton-data/var/log/proton-server/proton-server.log");
        candidates.emplace_back("./proton-data/server_logs/server.log"); /// legacy local cluster location
    }
    else if (config_key == "logger.errorlog")
    {
        candidates.emplace_back("./proton-data/var/log/proton-server/proton-server.err.log");
        candidates.emplace_back("./proton-data/server_logs/server.err.log"); /// legacy local cluster location
    }

    /// Try to derive a base directory from <path>.
    /// Note: in local cluster mode we may set <path> to ".../tables/", so we normalize back to the data root.
    if (config.has("path"))
    {
        fs::path base = fs::path{config.getString("path")};

        /// If path points to a "tables" subdir, treat its parent as data root.
        if (base.filename() == "tables")
            base = base.parent_path();

        /// Sometimes <path> is the data root with a trailing slash; keep both base and base/.. attempts minimal.
        const fs::path data_root = base;

        if (config_key == "logger.log")
        {
            candidates.emplace_back((data_root / "var/log/proton-server/proton-server.log").string());
            candidates.emplace_back((data_root / "server_logs/server.log").string());
        }
        else if (config_key == "logger.errorlog")
        {
            candidates.emplace_back((data_root / "var/log/proton-server/proton-server.err.log").string());
            candidates.emplace_back((data_root / "server_logs/server.err.log").string());
        }
    }

    for (const auto & candidate : candidates)
        if (exists(candidate))
            return candidate;

    /// As last resort, keep the configured path (even if missing) to preserve old behavior.
    return configured;
}

const char * proton_err_log_ddl = R"(CREATE EXTERNAL STREAM system.proton_err_log(
    raw string,
    _tp_time ALIAS to_time(extract(raw, '^(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+)')),
    level ALIAS extract(raw, '} <(\w+)>'),
    message ALIAS extract(raw, '} <\w+> (.*)'))
    SETTINGS
    type='log',
    log_files='LOG_FILE_VAR',
    log_dir='LOG_DIR_VAR',
    row_delimiter='(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{',
    timestamp_regex='^(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+)',
    local=true
    COMMENT 'version 1';)";

const char * proton_log_ddl = R"(CREATE EXTERNAL STREAM system.proton_log(
    raw string,
    _tp_time ALIAS to_time(extract(raw, '^(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+)')),
    level ALIAS extract(raw, '} <(\w+)>'),
    message ALIAS extract(raw, '} <\w+> (.*)'))
    SETTINGS
    type='log',
    log_files='LOG_FILE_VAR',
    log_dir='LOG_DIR_VAR',
    row_delimiter='(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{',
    timestamp_regex='^(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+)',
    local=true
    COMMENT 'version 1';)";

const char * task_execution_state_ddl = R"(CREATE STREAM system.task_execution_state(
    id uuid,
    version uint32,
    checkpoint string,
    last_execution_node uint32,
    last_execution_start datetime,
    last_execution_end datetime,
    last_execution_result string
)
TTL to_datetime(_tp_time) + INTERVAL 3 MONTH DELETE
PRIMARY KEY (id, version)
SETTINGS mode='versioned_kv'
COMMENT 'version 1';)";

}

void BuiltinSchemasProvisioner::addOtherBuiltinSchemas(const Poco::Util::AbstractConfiguration & config)
{
    struct LogInfo
    {
        const char * ddl;
        const char * name;
        const char * config_key;
    };

    /// Create schemas for both regular log and error log
    for (const auto & log_info :
         {LogInfo{proton_log_ddl, "proton_log", "logger.log"}, LogInfo{proton_err_log_ddl, "proton_err_log", "logger.errorlog"}})
    {
        auto log_path = resolveLogPath(config, log_info.config_key);
        if (log_path.empty())
            continue;

        std::filesystem::path log_file_path{log_path};
        auto log_file = log_file_path.filename();
        auto log_dir = log_file_path.remove_filename();

        auto schema = std::make_unique<Schema>();
        schema->name = log_info.name;

        String ddl{log_info.ddl};

        {
            String var{"LOG_FILE_VAR"};
            auto pos = ddl.find(var);
            chassert(pos != std::string::npos);
            ddl.replace(pos, var.size(), log_file.c_str());
        }

        {
            String var{"LOG_DIR_VAR"};
            auto pos = ddl.find(var);
            chassert(pos != std::string::npos);
            ddl.replace(pos, var.size(), log_dir.c_str());
        }

        schema->ddl.swap(ddl);

        schemas.push_back(std::move(schema));
    }

    /// Create schema for task execution
    auto schema = std::make_unique<Schema>();
    schema->name = "task_execution_state";
    schema->ddl = task_execution_state_ddl;
    schema->replace = false; /// need manual upgrade to avoid data loss
    schemas.push_back(std::move(schema));
}

BuiltinSchemasProvisioner::BuiltinSchemasProvisioner(ContextPtr context_, const Poco::Util::AbstractConfiguration & config)
    : WithContext(context_), logger(getLogger("BuiltinSchemasProvisioner"))
{
    try
    {
        addOtherBuiltinSchemas(config);
    }
    catch (const std::exception & ex)
    {
        LOG_INFO(logger, "Failed to add other builtin schemas, error='{}'", ex.what());
    }

    Poco::Util::AbstractConfiguration::Keys schema_names;
    config.keys(CONFIG_PREFIX, schema_names);

    for (const auto & name : schema_names)
    {
        auto prefix = fmt::format("{}.{}", CONFIG_PREFIX, name);
        if (config.getBool(prefix + ".enabled", true))
        {
            auto schema = std::make_unique<Schema>();
            schema->name = name;
            schema->ddl = config.getString(prefix + ".ddl", "");
            schemas.push_back(std::move(schema));
        }
    }

    loadFromResource();

    if (!schemas.empty())
        provision_thread.emplace([this] { provisionThreadFunction(); });
}

BuiltinSchemasProvisioner::~BuiltinSchemasProvisioner()
{
    stopped = true;
    if (provision_thread.has_value() && provision_thread->joinable())
        provision_thread->join();

    LOG_INFO(logger, "Dtored");
}

bool BuiltinSchemasProvisioner::parse(Schema & schema)
{
    const auto & ddl = schema.ddl;
    try
    {
        ParserCreateQuery parser;
        schema.create_query_ast
            = parseQuery(parser, ddl.data(), ddl.data() + ddl.size(), "SchemaProvisioner", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Can not parse query to provision schema: name={} error={} ddl='{}'", schema.name, ex.displayText(), ddl);
        return false;
    }

    auto * create_ptr = schema.create_query_ast->as<ASTCreateQuery>();
    if (create_ptr == nullptr)
    {
        LOG_ERROR(logger, "Not a create query: name={} ddl='{}'", schema.name, ddl);
        return false;
    }

    InterpreterCreateQuery::completeTableCreationAST(*create_ptr, Context::createCopy(getContext()));

    return true;
}

String BuiltinSchemasProvisioner::schema_names_in_resource[] = {
    "v_failed_mat_views", /// Tracks failed materialized views - useful for single-instance
    "v_mat_view_lags", /// Tracks materialized view processing lag - useful for single-instance
    "v_storages", /// Tracks storage usage - useful for single-instance
};

void BuiltinSchemasProvisioner::loadFromResource()
{
    for (const auto & schema_name : schema_names_in_resource)
    {
        String resource_name = schema_name + ".sql";
        auto query = getResource(resource_name);
        if (query.empty())
        {
            LOG_WARNING(logger, "Builtin schema is not found in resource: name={}", schema_name);
            continue;
        }

        auto schema = std::make_unique<Schema>();
        schema->name = schema_name;
        schema->ddl = query;
        schema->replace = true;
        schemas.push_back(std::move(schema));
    }
}

void BuiltinSchemasProvisioner::provisionThreadFunction()
{
    setThreadName("Provisioner");

    auto this_context = getContext();

    while (!stopped)
    {
        for (auto iter = schemas.begin(); iter != schemas.end();)
        {
            auto & schema = *iter;

            if (!schema->create_query_ast && !parse(*schema))
            {
                /// Parse ddl failed
                iter = schemas.erase(iter);
                continue;
            }

            const auto * create_query_ast = schema->create_query_ast->as<ASTCreateQuery>();

            StorageID table_id{create_query_ast->getDatabase(), create_query_ast->getTable()};
            if (DatabaseCatalog::instance().tryGetTable(table_id, this_context))
            {
                /// schema already created
                if (!schema->replace || !create_query_ast->replace_view)
                {
                    iter = schemas.erase(iter);
                    continue;
                }

                /// Check whether schema is changed by comparing COMMENT.
                /// Only replace when DDL COMMENT is changed in the new schema version.
                DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
                ASTPtr old_ast = database->getCreateTableQuery(table_id.table_name, this_context);
                const auto * old_create_query_ast = old_ast->as<ASTCreateQuery>();
                if (create_query_ast->comment != nullptr && old_create_query_ast->comment != nullptr
                    && create_query_ast->comment->formatForLogging() == old_create_query_ast->comment->formatForLogging())
                {
                    iter = schemas.erase(iter);
                    continue;
                }
            }

            {
                try
                {
                    auto query_context = Context::createCopy(this_context);
                    query_context->makeQueryContext();
                    InterpreterCreateQuery interpreter(schema->create_query_ast, query_context);
                    interpreter.setInternal(true);
                    interpreter.execute();

                    LOG_INFO(
                        logger,
                        "Builtin schema provisioned: database={} table={}",
                        create_query_ast->getDatabase(),
                        create_query_ast->getTable());

                    iter = schemas.erase(iter);
                    continue;
                }
                catch (const DB::Exception & ex)
                {
                    LOG_ERROR(
                        logger,
                        "Failed to create builtin schema: database={} table={} error={}",
                        create_query_ast->getDatabase(),
                        create_query_ast->getTable(),
                        ex.displayText());

                    auto err_code = ex.code();
                    bool is_permanent_error = (err_code == ErrorCodes::BAD_ARGUMENTS || err_code == ErrorCodes::SYNTAX_ERROR);
                    if (is_permanent_error)
                        iter = schemas.erase(iter);
                }
            }

            ++iter;
        }

        if (schemas.empty())
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

}
