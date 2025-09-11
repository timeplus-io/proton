#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>

/// proton: starts
#include <Common/LogLevels.h>
/// proton: ends

#include <magic_enum.hpp>

#include <unordered_set>

/// proton: starts.
namespace
{
const std::unordered_set<std::string_view> ENABLED_COMMANDS{
    /// Clickhouse old command
    "STOP_MERGES",
    "START_MERGES",
    "STOP_TTL_MERGES",
    "START_TTL_MERGES",
    "FLUSH_LOGS",
    "DROP_MMAP_CACHE",
    "DROP_COMPILED_EXPRESSION_CACHE",

    /// For stream recovery
    "PAUSE_STREAM",
    "RECOVER_STREAM",
    "RESUME_STREAM",

    /// For materialized view recovery
    "PAUSE_MATERIALIZED_VIEW",
    "RESUME_MATERIALIZED_VIEW",
    "ABORT_MATERIALIZED_VIEW",
    "RECOVER_MATERIALIZED_VIEW",

    "RELOAD_DICTIONARY",

    "DROP_FORMAT_SCHEMA_CACHE",

    "SET_LOG_LEVEL",

    "SHOW_LOGGERS",

    "PAUSE_TASK",
    "RESUME_TASK",
    "EXECUTE_TASK",

    "INSTALL_PYTHON_PACKAGE",
    "UNINSTALL_PYTHON_PACKAGE",
    "LIST_PYTHON_PACKAGES",
};
}
/// proton: ends.

namespace ErrorCodes
{
}


namespace DB
{

static bool parseQueryWithOnClusterAndMaybeTable(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                                 Expected & expected, bool require_table, bool allow_string_literal)
{
    /// Better form for user: SYSTEM <ACTION> table ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster table
    /// Need to support both
    String cluster;

    /// proton: starts
    /// we don't need to parse ON CLUSTER
    /// bool parsed_on_cluster = false;
    /// if (ParserKeyword{"ON"}.ignore(pos, expected))
    /// {
    ///     if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
    ///         return false;
    ///     parsed_on_cluster = true;
    /// }
    /// proton: ends

    bool parsed_table = false;
    if (allow_string_literal)
    {
        ASTPtr ast;
        if (ParserStringLiteral{}.parse(pos, ast, expected))
        {
            res->setTable(ast->as<ASTLiteral &>().value.safeGet<String>());
            parsed_table = true;
        }
    }

    if (!parsed_table)
        parsed_table = parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);

    if (!parsed_table && require_table)
        return false;

    /// proton: starts
    /// we don't need to parse ON CLUSTER
    /// if (!parsed_on_cluster && ParserKeyword{"ON"}.ignore(pos, expected))
    ///     if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
    ///         return false;
    /// proton: ends

    res->cluster = cluster;

    if (res->database)
        res->children.push_back(res->database);
    if (res->table)
        res->children.push_back(res->table);

    return true;
}

static bool parseQueryWithOnCluster(std::shared_ptr<ASTSystemQuery> & res [[maybe_unused]], IParser::Pos & pos [[maybe_unused]],
                                    [[maybe_unused]] Expected & expected)
{
    /// proton: starts
    /// we don't need to parse ON CLUSTER
    return false;
    /// String cluster_str;
    /// if (ParserKeyword{"ON"}.ignore(pos, expected))
    /// {
    ///     if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
    ///         return false;
    /// }
    /// res->cluster = cluster_str;

    /// return true;
    /// proton: ends
}

bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos, expected))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;

    for (const auto & type : magic_enum::enum_values<Type>())
    {
        if (ParserKeyword{ASTSystemQuery::typeToString(type)}.ignore(pos, expected))
        {
            res->type = type;
            found = true;
            break;
        }
    }

    /// proton: starts. Support unpause as alias for resume
    if (!found)
    {
        if (ParserKeyword{"UNPAUSE STREAM"}.ignore(pos, expected))
            res->type = Type::RESUME_STREAM;
        else if (ParserKeyword{"UNPAUSE MATERIALIZED VIEW"}.ignore(pos, expected))
            res->type = Type::RESUME_MATERIALIZED_VIEW;
        else if (ParserKeyword{"UNPAUSE TASK"}.ignore(pos, expected))
            res->type = Type::RESUME_TASK;
        else
            return false;
    }

    if (!ENABLED_COMMANDS.contains(magic_enum::enum_name(res->type)))
        return false;
    /// proton: ends.

    switch (res->type)
    {
        case Type::RELOAD_DICTIONARY:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ true))
                return false;
            break;
        }
        case Type::RELOAD_MODEL:
        {
            parseQueryWithOnCluster(res, pos, expected);

            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->target_model = ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
            {
                ParserIdentifier model_parser;
                ASTPtr model;
                String target_model;

                if (!model_parser.parse(pos, model, expected))
                    return false;

                if (!tryGetIdentifierNameInto(model, res->target_model))
                    return false;
            }

            break;
        }
        case Type::RELOAD_FUNCTION:
        {
            parseQueryWithOnCluster(res, pos, expected);

            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->target_function = ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
            {
                ParserIdentifier function_parser;
                ASTPtr function;
                String target_function;

                if (!function_parser.parse(pos, function, expected))
                    return false;

                if (!tryGetIdentifierNameInto(function, res->target_function))
                    return false;
            }

            break;
        }
        case Type::DROP_REPLICA:
        {
            parseQueryWithOnCluster(res, pos, expected);

            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (ParserKeyword{"FROM"}.ignore(pos, expected))
            {
                // way 1. parse replica database
                // way 2. parse replica tables
                // way 3. parse replica zkpath
                if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                {
                    ParserIdentifier database_parser;
                    if (!database_parser.parse(pos, res->database, expected))
                        return false;
                }
                else if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                {
                    parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
                }
                else if (ParserKeyword{"ZKPATH"}.ignore(pos, expected))
                {
                    ASTPtr path_ast;
                    if (!ParserStringLiteral{}.parse(pos, path_ast, expected))
                        return false;
                    String zk_path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
                    if (!zk_path.empty() && zk_path[zk_path.size() - 1] == '/')
                        zk_path.pop_back();
                    res->replica_zk_path = zk_path;
                }
                else
                    return false;
            }
            else
                res->is_drop_whole_replica = true;

            break;
        }

        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
        case Type::WAIT_LOADING_PARTS:
        {
            parseQueryWithOnCluster(res, pos, expected);
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            break;
        }
        case Type::RESTART_DISK:
        {
            parseQueryWithOnCluster(res, pos, expected);

            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->disk = ast->as<ASTIdentifier &>().name();
            else
                return false;

            break;
        }
        /// FLUSH DISTRIBUTED requires table
        /// START/STOP DISTRIBUTED SENDS does not require table
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ false, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::FLUSH_DISTRIBUTED:
        case Type::RESTORE_REPLICA:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::STOP_MERGES:
        case Type::START_MERGES:
        {
            String storage_policy_str;
            String volume_str;

            auto parse_on_volume = [&]() -> bool
            {
                ASTPtr ast;
                if (ParserIdentifier{}.parse(pos, ast, expected))
                    storage_policy_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;

                if (!ParserToken{TokenType::Dot}.ignore(pos, expected))
                    return false;

                if (ParserIdentifier{}.parse(pos, ast, expected))
                    volume_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;

                return true;
            };

            if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
            {
                if (!parse_on_volume())
                    return false;
            }
            else
            {
                parseQueryWithOnCluster(res, pos, expected);
                if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
                {
                    if (!parse_on_volume())
                        return false;
                }
            }

            res->storage_policy = storage_policy_str;
            res->volume = volume_str;
            if (res->volume.empty() && res->storage_policy.empty())
                parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
            break;
        }

        case Type::STOP_TTL_MERGES:
        case Type::START_TTL_MERGES:
        case Type::STOP_MOVES:
        case Type::START_MOVES:
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATED_SENDS:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
            parseQueryWithOnCluster(res, pos, expected);
            parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
            break;

        case Type::SUSPEND:
        {
            parseQueryWithOnCluster(res, pos, expected);

            ASTPtr seconds;
            if (!(ParserKeyword{"FOR"}.ignore(pos, expected)
                && ParserUnsignedInteger().parse(pos, seconds, expected)
                && ParserKeyword{"SECOND"}.ignore(pos, expected)))   /// SECOND, not SECONDS to be consistent with INTERVAL parsing in SQL
            {
                return false;
            }

            res->seconds = seconds->as<ASTLiteral>()->value.get<UInt64>();
            break;
        }
        case Type::DROP_FILESYSTEM_CACHE:
        {
            ParserLiteral path_parser;
            ASTPtr ast;
            if (path_parser.parse(pos, ast, expected))
                res->filesystem_cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
            break;
        }
        case Type::DROP_SCHEMA_CACHE:
        {
            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                if (ParserKeyword{"FILE"}.ignore(pos, expected))
                    res->schema_cache_storage = "FILE";
                else if (ParserKeyword{"S3"}.ignore(pos, expected))
                    res->schema_cache_storage = "S3";
                else if (ParserKeyword{"HDFS"}.ignore(pos, expected))
                    res->schema_cache_storage = "HDFS";
                else if (ParserKeyword{"URL"}.ignore(pos, expected))
                    res->schema_cache_storage = "URL";
                else
                    return false;
            }
            break;
        }
        case Type::DROP_FORMAT_SCHEMA_CACHE:
        {
                if (ParserKeyword{"FOR"}.ignore(pos, expected))
                {
                    if (ParserKeyword{"Protobuf"}.ignore(pos, expected))
                        res->schema_cache_format = "Protobuf";
                    else
                        return false;
                }
                break;
        }
        /// proton : starts
        case Type::PAUSE_STREAM:
            [[fallthrough]];
        case Type::RESUME_STREAM:
            [[fallthrough]];
        case Type::RECOVER_STREAM:
        {
            /// SYSTEM RECOVER STREAM database.stream shard_id
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;

            ASTPtr shard_ast;
            if (!ParserLiteral{}.parse(pos, shard_ast, expected))
                return false;

            res->shard = static_cast<UInt32>(shard_ast->as<ASTLiteral &>().value.safeGet<UInt32>());
            break;
        }
        case Type::PAUSE_MATERIALIZED_VIEW:
            [[fallthrough]];
        case Type::RESUME_MATERIALIZED_VIEW:
            [[fallthrough]];
        case Type::ABORT_MATERIALIZED_VIEW:
            [[fallthrough]];
        case Type::RECOVER_MATERIALIZED_VIEW:
        {
            /// SYSTEM PAUSE|RESUME|ABORT|RECOVER MATERIALIZED VIEW database.mv [LOCAL]
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;

            if (ParserKeyword{"PERMANENT"}.ignore(pos, expected))
                res->is_permanent = true;
            break;
        }
        case Type::SET_LOG_LEVEL:
        {
            /// SYSTEM SET LOG LEVEL '<log_level>' [FOR '<logger_name>']
            /// SYSTEM SET LOG LEVEL <log_level> [FOR '<logger_name>']
            ASTPtr level_ast;

            if (!ParserStringLiteral{}.parse(pos, level_ast, expected))
            {
                if (!ParserIdentifier{}.parse(pos, level_ast, expected))
                    return false;

                String identifier_name;
                tryGetIdentifierNameInto(level_ast, identifier_name);
                res->log_level = identifier_name;
            }
            else
            {
                res->log_level = level_ast->as<ASTLiteral &>().value.safeGet<String>();
            }

            String lower_level = res->log_level;
            std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(), [](unsigned char c) { return std::tolower(c); });

            if (!getValidLogLevels().contains(lower_level))
            {
                expected.add(pos, "valid log level (none, fatal, critical, error, warning, notice, information, debug, trace, test)");
                return false;
            }

            res->log_level = lower_level;

            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                ASTPtr logger_ast;
                if (!ParserStringLiteral{}.parse(pos, logger_ast, expected))
                    return false;

                res->logger_name = logger_ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            break;
        }
        case Type::SHOW_LOGGERS:
        {
            /// SYSTEM SHOW LOGGERS
            break;
        }
        case Type::PAUSE_TASK:
            [[fallthrough]];
        case Type::RESUME_TASK:
            [[fallthrough]];
        case Type::EXECUTE_TASK:
        {
            /// SYSTEM PAUSE/RESUME/EXECUTE TASK ns.task_name
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            break;
        }
        case Type::INSTALL_PYTHON_PACKAGE:
        {
            /// SYSTEM INSTALL PYTHON PACKAGE 'package_name' ['version']
            ASTPtr package_ast;
            if (!ParserStringLiteral{}.parse(pos, package_ast, expected))
                return false;

            res->python_package_name = package_ast->as<ASTLiteral &>().value.safeGet<String>();

            /// Optional version parameter
            ASTPtr version_ast;
            if (ParserStringLiteral{}.parse(pos, version_ast, expected))
            {
                res->python_package_version = version_ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            break;
        }
        case Type::UNINSTALL_PYTHON_PACKAGE:
        {
            /// SYSTEM UNINSTALL PYTHON PACKAGE 'package_name'
            ASTPtr package_ast;
            if (!ParserStringLiteral{}.parse(pos, package_ast, expected))
                return false;

            res->python_package_name = package_ast->as<ASTLiteral &>().value.safeGet<String>();
            break;
        }
        case Type::LIST_PYTHON_PACKAGES:
        {
            /// SYSTEM LIST PYTHON PACKAGES
            break;
        }
        /// proton : ends
        default:
        {
            parseQueryWithOnCluster(res, pos, expected);
            break;
        }
    }

    if (res->database)
        res->children.push_back(res->database);
    if (res->table)
        res->children.push_back(res->table);

    node = std::move(res);
    return true;
}

}
