#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/executeSelectQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Cluster/Requests/ChangeLogLevelRequest.h>
#include <Columns/ColumnString.h>
#include <Common/Jemalloc.h>
#include <DataTypes/DataTypeString.h>
#include <Loggers/Loggers.h>
#include <Parsers/ASTSystemQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>
#include <Poco/Logger.h>
#include <Common/LogLevels.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_STREAM;
extern const int REPLICA_IS_NOT_IN_QUORUM;
}

void InterpreterSystemQuery::executeMaterializedViewAdmission(const ASTSystemQuery & system)
{
    auto query_context = getContext();
    std::string_view admission_action = ASTSystemQuery::typeToString(system.type);

    auto full_name = table_id.getFullTableName();
    auto mv = StorageMaterializedView::tryGetStorage(table_id, query_context);
    if (!mv)
        throw Exception(ErrorCodes::UNKNOWN_STREAM, "Materialized view {} is not found", full_name);

    bool is_permanent = system.is_permanent;
    auto stream_shard = mv->getCkptLogStreamShard();

    stream_shard.id_shard.shard = 0;

    cluster::Error err;

    using Type = ASTSystemQuery::Type;
    switch (system.type)
    {
        case Type::PAUSE_MATERIALIZED_VIEW:
        {
            Stopwatch stopwatch;
            int result = mv->pause(/*timeout_ms=*/10'000);
            if (result != 0)
            {
                err.error_code = result;
                err.error_message = fmt::format("Failed to pause materialized view {}", full_name);
                break;
            }

            auto paused_elapsed_ms = stopwatch.elapsedMilliseconds();
            if (is_permanent)
            {
                try
                {
                    executeNonInsertQuery(
                        fmt::format("ALTER STREAM {} MODIFY QUERY SETTING pause_on_start=true", full_name), getContext(), [](Block &&) { });
                }
                catch (...)
                {
                    tryLogCurrentException(log, fmt::format("Failed to set setting pause_on_start=true for '{}'", full_name));
                    err.error_code = getCurrentExceptionCode();
                }

                auto total_elapsed_ms = stopwatch.elapsedMilliseconds();
                LOG_INFO(
                    log,
                    "Pausing mv={}.{} took {}ms, update mv metadata took={}ms",
                    table_id.database_name,
                    table_id.table_name,
                    paused_elapsed_ms,
                    total_elapsed_ms - paused_elapsed_ms);
            }
            else
            {
                LOG_INFO(log, "Pausing mv={}.{} took {}ms", table_id.database_name, table_id.table_name, paused_elapsed_ms);
            }
            break;
        }
        case Type::RESUME_MATERIALIZED_VIEW:
        {
            Stopwatch stopwatch;
            int result = mv->resume();
            if (result != 0)
            {
                err.error_code = result;
                err.error_message = fmt::format("Failed to resume materialized view {}", full_name);
                break;
            }

            auto resumed_elapsed_ms = stopwatch.elapsedMilliseconds();
            if (is_permanent)
            {
                try
                {
                    executeNonInsertQuery(
                        fmt::format("ALTER STREAM {} RESET QUERY SETTING pause_on_start", full_name), getContext(), [](Block &&) { });
                }
                catch (...)
                {
                    tryLogCurrentException(log, fmt::format("Failed to reset setting pause_on_start for '{}'", full_name));
                    err.error_code = getCurrentExceptionCode();
                }

                auto total_elapsed_ms = stopwatch.elapsedMilliseconds();
                LOG_INFO(
                    log,
                    "Resuming mv={}.{} took {}ms, update mv metadata took={}ms",
                    table_id.database_name,
                    table_id.table_name,
                    resumed_elapsed_ms,
                    total_elapsed_ms - resumed_elapsed_ms);
            }
            else
            {
                LOG_INFO(log, "Resuming mv={}.{} took {}ms", table_id.database_name, table_id.table_name, resumed_elapsed_ms);
            }
            break;
        }
        case Type::ABORT_MATERIALIZED_VIEW:
        {
            Stopwatch stopwatch;
            int result = mv->abort();
            if (result != 0)
            {
                err.error_code = result;
                err.error_message = fmt::format("Failed to abort materialized view {}", full_name);
            }
            LOG_INFO(log, "Abort mv={}.{} took {}ms", table_id.database_name, table_id.table_name, stopwatch.elapsedMilliseconds());
            break;
        }
        case Type::RECOVER_MATERIALIZED_VIEW:
        {
            Stopwatch stopwatch;
            int result = mv->recover();
            if (result != 0)
            {
                err.error_code = result;
                err.error_message = fmt::format("Failed to recover materialized view {}", full_name);
            }
            LOG_INFO(log, "Recover mv={}.{} took {}ms", table_id.database_name, table_id.table_name, stopwatch.elapsedMilliseconds());
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of SYSTEM query '{}'", admission_action);
        }
    }

    if (err.hasError())
        throw Exception(
            err.error_code,
            "Failed to {} '{}.{}' {} : {}",
            admission_action,
            table_id.database_name,
            table_id.table_name,
            system.is_permanent ? " PERMANENT" : "",
            err.error_message);
}

void InterpreterSystemQuery::doExecuteStreamAdmission(const ASTSystemQuery & system, std::string_view admission_action)
{
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_STREAM, "Stream {}.{} is not found", table_id.database_name, table_id.table_name);
    }

    cluster::StreamIDShard stream_shard{table_id.uuid, system.shard};

    cluster::Error err;
    auto & native_log = Globals::getNativeLog();

    using Type = ASTSystemQuery::Type;
    if (system.type == Type::PAUSE_STREAM)
        err = native_log.pauseStreamShard(stream_shard);
    else if (system.type == Type::RESUME_STREAM)
        err = native_log.resumeStreamShard(stream_shard);
    else if (system.type == Type::RECOVER_STREAM)
        err = native_log.recoverStreamShard(stream_shard);

    if (err.hasError())
        throw Exception(
            err.error_code,
            "Failed to {} stream '{}.{}' shard={} error_message={}",
            admission_action,
            table_id.database_name,
            table_id.table_name,
            system.shard,
            err.error_message);
}

void InterpreterSystemQuery::executePauseStream(const ASTSystemQuery & system)
{
    doExecuteStreamAdmission(system, "pause");
}

void InterpreterSystemQuery::executeResumeStream(const ASTSystemQuery & system)
{
    doExecuteStreamAdmission(system, "resume");
}

void InterpreterSystemQuery::executeRecoverStream(const ASTSystemQuery & system)
{
    doExecuteStreamAdmission(system, "recover");
}

void InterpreterSystemQuery::executeSetLogLevel(const ASTSystemQuery & query)
{
    if (query.log_level.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Log level cannot be empty");

    std::string lower_level = query.log_level;
    std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(), [](unsigned char c) { return std::tolower(c); });

    if (!getValidLogLevels().contains(lower_level))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid log level '{}'. Valid levels are: {}",
            query.log_level,
            fmt::join(getValidLogLevels(), ", "));
    }

    if (!query.logger_name.empty())
    {
        if (!Poco::Logger::has(query.logger_name))
        {
            /// If exact match fails, try pattern matching as fallback
            auto matched_loggers = Loggers::findLoggersByPattern(query.logger_name);

            if (matched_loggers.empty())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "No loggers found matching pattern '{}'. Use `SYSTEM SHOW LOGGERS` to check existing loggers.",
                    query.logger_name);
            }
        }
    }

    LOG_INFO(log, "Changing log level to '{}' for logger '{}'", lower_level, query.logger_name.empty() ? "all" : query.logger_name);

    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    auto change_log_level_req = std::make_shared<cluster::ChangeLogLevelRequest>(
        std::string(lower_level),
        std::string(query.logger_name),
        0,
        timeout_ms,
        /*request_version_=*/1);

    /// Send request to meta log for cluster-wide distribution
    LOG_INFO(
        log,
        "Sending cluster-wide log level change request: level='{}', logger='{}'",
        lower_level,
        query.logger_name.empty() ? "all" : query.logger_name);

    auto system_command_resp = metastore.changeLogLevel(std::move(change_log_level_req));

    LOG_INFO(log, "Log level change request processed");

    if (system_command_resp->hasError())
    {
        const auto & err = system_command_resp->error();
        throw Exception(
            err.error_code,
            "Failed to change log level to '{}' for logger '{}': (error_code={}, error_message={})",
            lower_level,
            query.logger_name.empty() ? "all loggers" : query.logger_name,
            err.error_code,
            err.error_message);
    }
}

void InterpreterSystemQuery::executeJemallocControl(const ASTSystemQuery & system)
{
#if !USE_JEMALLOC
    (void)system;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Jemalloc support is not enabled in this build");
#else
    auto type = system.type;
    switch (type)
    {
        case ASTSystemQuery::Type::JEMALLOC_PURGE:
            purgeJemallocArenas();
            break;
        case ASTSystemQuery::Type::JEMALLOC_ENABLE_PROFILE:
            setJemallocProfileActive(true);
            break;
        case ASTSystemQuery::Type::JEMALLOC_DISABLE_PROFILE:
            setJemallocProfileActive(false);
            break;
        case ASTSystemQuery::Type::JEMALLOC_FLUSH_PROFILE:
            /// Explicitly requested by the user, so the dump location is worth logging.
            flushJemallocProfile("/tmp/jemalloc_proton", /*log=*/ true);
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported jemalloc operation type {}", ASTSystemQuery::typeToString(type));
    }
#endif
}

BlockIO InterpreterSystemQuery::executeShowLoggers(const ASTSystemQuery &)
{
    MutableColumns result_columns = MutableColumns(2);
    result_columns[0] = ColumnString::create();
    result_columns[1] = ColumnString::create();

    std::vector<std::string> all_loggers;
    all_loggers.reserve(512);
    Poco::Logger::root().names(all_loggers);
    std::sort(all_loggers.begin(), all_loggers.end());

    result_columns[0]->insert("root");
    result_columns[1]->insert(String(InternalTextLogsQueue::getPriorityName(Poco::Logger::root().getLevel())));

    for (const auto & logger_name : all_loggers)
    {
        if (!logger_name.empty())
        {
            try
            {
                auto logger = getLogger(logger_name);
                result_columns[0]->insert(logger_name);
                result_columns[1]->insert(String(InternalTextLogsQueue::getPriorityName(logger->getLevel())));
            }
            catch (...)
            {
                continue;
            }
        }
    }

    Block result_block = Block{
        {std::move(result_columns[0]), std::make_shared<DataTypeString>(), "logger_name"},
        {std::move(result_columns[1]), std::make_shared<DataTypeString>(), "level"}};

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result_block)));
    return res;
}
}
