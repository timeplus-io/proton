#include "DDLHelper.h"

#include <DistributedMetadata/TaskStatusService.h>
#include "ASTToJSONUtils.h"

#include <DistributedMetadata/CatalogService.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int STREAM_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYNTAX_ERROR;
}

namespace
{
    const std::vector<String> CREATE_TABLE_SETTINGS = {
        "logstore_cluster_id",
        "logstore_subscription_mode",
        "logstore_auto_offset_reset",
        "logstore_request_required_acks",
        "logstore_request_timeout_ms",
        "logstore_retention_bytes",
        "logstore_retention_ms",
        "logstore_flush_messages",
        "logstore_flush_ms",
        "logstore_replication_factor",
        "distributed_ingest_mode",
        "distributed_flush_threshold_ms",
        "distributed_flush_threshold_count",
        "distributed_flush_threshold_bytes",
        "storage_type",
        "logstore",
        "mode",
    };
}

void getAndValidateStorageSetting(
    std::function<String(const String &)> get_setting, std::function<void(const String &, const String &)> handle_setting)
{
    for (const auto & key : CREATE_TABLE_SETTINGS)
    {
        const auto & value = get_setting(key);
        if (value.empty())
            continue;

        if (key == "logstore_subscription_mode")
        {
            if (value != "shared" && value != "dedicated")
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "logstore_subscription_mode only supports 'shared' or 'dedicated'");
        }
        else if (key == "logstore_auto_offset_reset")
        {
            if (value != "earliest" && value != "latest")
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "logstore_auto_offset_reset only supports 'earliest' or 'latest'");
        }
        else if (key == "distributed_ingest_mode")
        {
            if (value != "async" && value != "sync" && value != "fire_and_forget" && value != "ordered")
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "distributed_ingest_mode only supports 'async' or 'sync' or 'fire_and_forget' or 'ordered'");
        }
        else if (key == "storage_type")
        {
            if (value != "hybrid" && value != "streaming")
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "storage_type only supports 'hybrid' or 'streaming'");
        }

        handle_setting(key, value);
    }
}

ASTPtr functionToAST(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserFunction parser;
    return parseQuery(parser, start, end, "", 0, 0);
}

void prepareEngine(ASTCreateQuery & create, ContextPtr ctx)
{
    Field shards = ctx->getStreamSettings().default_shards.value;
    Field replicas = ctx->getStreamSettings().default_replicas.value;
    Field sharding_expr_field = ctx->getStreamSettings().default_sharding_expr.value;
    String expr;

    if (!create.storage)
    {
        create.set(create.storage, std::make_shared<ASTStorage>());
    }
    else if (create.storage->settings && !create.storage->settings->changes.empty())
    {
        create.storage->settings->changes.tryGet("shards", shards);
        create.storage->settings->changes.tryGet("replicas", replicas);
        create.storage->settings->changes.tryGet("sharding_expr", sharding_expr_field);
    }

    sharding_expr_field.tryGet<String>(expr);
    ASTPtr sharding_expr = functionToAST(expr);

    auto engine = makeASTFunction("Stream", std::make_shared<ASTLiteral>(shards), std::make_shared<ASTLiteral>(replicas), sharding_expr);
    create.storage->set(create.storage->engine, engine);
}

void prepareEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx)
{
    Poco::URI uri;

    getAndValidateStorageSetting(
        [&](const String & key) {
            if (!create.storage || !create.storage->settings)
                return String();

            Field field_value("");
            create.storage->settings->changes.tryGet(key, field_value);

            String value;
            field_value.tryGet<String>(value);
            return value;
        },
        [&](const String & key, const String & value) { uri.addQueryParameter(key, value); });

    /// Compose URL params for key values
    ctx->setQueryParameter("url_parameters", uri.getRawQuery());
}

void prepareColumns(ASTCreateQuery & create)
{
    /// columns_list should contains valid column definition
    if (!create.columns_list || !create.columns_list->columns || create.columns_list->columns->children.empty())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Columns is empty no column has been defined.");

    ASTs & column_asts = create.columns_list->columns->children;
    Field event_time_default = ProtonConsts::DEFAULT_EVENT_TIME;

    if (create.storage->settings && !create.storage->settings->changes.empty())
        create.storage->settings->changes.tryGet("event_time_column", event_time_default);

    String event_time_default_expr;
    event_time_default.tryGet<String>(event_time_default_expr);

    bool has_event_time = false;
    bool has_index_time = false;
    bool has_sequence_id = false;
    bool has_delta_flag = false;

    for (const ASTPtr & column_ast : column_asts)
    {
        const auto & column = column_ast->as<ASTColumnDeclaration &>();

        /// Skip reserved internal columns
        if (column.name.starts_with("_tp_")
            || std::find(ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.begin(), ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end(), column.name)
                != ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end())
        {
            if (ProtonConsts::RESERVED_EVENT_TIME == column.name)
            {
                has_event_time = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "datetime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'datetime64' but actual type '{}'.",
                        ProtonConsts::RESERVED_EVENT_TIME,
                        column.type->getID());
            }
            else if (ProtonConsts::RESERVED_INDEX_TIME == column.name)
            {
                has_index_time = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "datetime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'datetime64' but actual type '{}'.",
                        ProtonConsts::RESERVED_INDEX_TIME,
                        column.type->getID());
            }
            else if (ProtonConsts::RESERVED_EVENT_SEQUENCE_ID == column.name)
            {
                has_sequence_id = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "int64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'int64 ' but actual type '{}'.",
                        ProtonConsts::RESERVED_EVENT_SEQUENCE_ID,
                        column.type->getID());
            }
            else if (ProtonConsts::RESERVED_DELTA_FLAG == column.name)
            {
                has_delta_flag = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "int8")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'int8' but actual type '{}'.",
                        ProtonConsts::RESERVED_DELTA_FLAG,
                        column.type->getID());
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column {} is reserved, should not used in create query.", column.name);
        }
    }

    if (!has_event_time)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = ProtonConsts::RESERVED_EVENT_TIME;
        col_tp_time->type
            = makeASTFunction("datetime64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));

        if (!event_time_default_expr.empty())
        {
            col_tp_time->default_specifier = "DEFAULT";
            ParserTernaryOperatorExpression expr_parser;

            const char * start = event_time_default_expr.data();
            const char * end = start + event_time_default_expr.size();
            col_tp_time->default_expression = parseQuery(expr_parser, start, end, "", 0, 0);
            col_tp_time->children.push_back(col_tp_time->default_expression);
        }

        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        auto func_double_delta = std::make_shared<ASTFunction>();
        func_double_delta->name = "DoubleDelta";
        auto func_lz4 = std::make_shared<ASTFunction>();
        func_lz4->name = "LZ4";
        col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_lz4));
        column_asts.emplace_back(std::move(col_tp_time));
    }

    if (!has_index_time)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = ProtonConsts::RESERVED_INDEX_TIME;
        col_tp_time->type
            = makeASTFunction("datetime64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));
        /// index time is the timestamp indexed to historical store. Don't specify a default value expression here to save disk space in streaming store
        /// since light ingest will ignore this column completely
        /// col_tp_time->default_specifier = "DEFAULT";
        /// col_tp_time->default_expression
        ///    = makeASTFunction("now64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));
        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        auto func_double_delta = std::make_shared<ASTFunction>();
        func_double_delta->name = "DoubleDelta";
        auto func_lz4 = std::make_shared<ASTFunction>();
        func_lz4->name = "LZ4";
        col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_lz4));
        column_asts.emplace_back(std::move(col_tp_time));
    }

    (void)has_sequence_id;
#if 0
    if (!has_sequence_id)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = ProtonConsts::RESERVED_EVENT_SEQUENCE_ID;
        col_tp_time->type = makeASTFunction("int64");
        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        auto func_delta = std::make_shared<ASTFunction>();
        func_delta->name = "Delta";
        auto func_lz4 = std::make_shared<ASTFunction>();
        func_lz4->name = "LZ4";
        col_tp_time->codec = makeASTFunction("CODEC", std::move(func_delta), std::move(func_lz4));
        column_asts.emplace_back(std::move(col_tp_time));
    }
#endif

    /// Only changelog stream needs delta flag
    if (!has_delta_flag)
    {
        Field mode("");
        if (create.storage->settings)
            create.storage->settings->changes.tryGet("mode", mode);

        if (mode == ProtonConsts::CHANGELOG_MODE || mode == ProtonConsts::CHANGELOG_KV_MODE)
        {
            auto delta_flag = std::make_shared<ASTColumnDeclaration>();
            delta_flag->name = ProtonConsts::RESERVED_DELTA_FLAG;
            delta_flag->type = makeASTFunction("int8");
            delta_flag->default_specifier = "DEFAULT";
            delta_flag->default_expression = std::make_shared<ASTLiteral>(Field(Int8(1)));
            delta_flag->children.push_back(delta_flag->default_expression);
            column_asts.emplace_back(std::move(delta_flag));
        }
    }
}

void prepareOrderByAndPartitionBy(ASTCreateQuery & create)
{
    /// FIXME: raw table might have different order by and partition by
    Field mode("");
    if (create.storage->settings)
        create.storage->settings->changes.tryGet("mode", mode);

    if (mode == ProtonConsts::VERSIONED_KV_MODE || mode == ProtonConsts::CHANGELOG_KV_MODE)
    {
        if (!create.storage->primary_key)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Primary key is required for changelog kv or versioned kv stream");
    }
    else
    {
        if (!create.storage->order_by && !create.storage->primary_key)
        {
            auto new_order_by = makeASTFunction("to_start_of_hour", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
            create.storage->set(create.storage->order_by, new_order_by);
        }
    }

    if (!create.storage->partition_by)
    {
        auto new_partition_by = makeASTFunction("to_YYYYMMDD", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
        create.storage->set(create.storage->partition_by, new_partition_by);
    }
}

void prepareCreateQueryForStream(ASTCreateQuery & create)
{
    prepareColumns(create);
    prepareOrderByAndPartitionBy(create);
}

void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list)
{
    const auto & columns_ast = columns_list->columns;
    Poco::JSON::Array columns_mapping_json;
    auto new_columns = std::make_shared<ASTColumns>();

    for (auto & ast_it : columns_ast->children)
    {
        const auto & col_decl = ast_it->as<ASTColumnDeclaration &>();
        Poco::JSON::Object column_mapping_json;
        ColumnDeclarationToJSON(column_mapping_json, col_decl);
        columns_mapping_json.add(column_mapping_json);
    }
    resp_table.set("columns", columns_mapping_json);
}

nlog::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams)
{
    if (queryParams.contains("column"))
    {
        auto iter = queryParams.find("query_method");

        if (iter->second == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return nlog::OpCode::CREATE_COLUMN;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return nlog::OpCode::ALTER_COLUMN;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return nlog::OpCode::DELETE_COLUMN;
        }
        else
        {
            assert(false);
            return nlog::OpCode::MAX_OPS_CODE;
        }
    }

    return nlog::OpCode::ALTER_TABLE;
}

std::map<ASTAlterCommand::Type, nlog::OpCode> command_type_to_opCode
    = {{ASTAlterCommand::Type::ADD_COLUMN, nlog::OpCode::CREATE_COLUMN},
       {ASTAlterCommand::Type::MODIFY_COLUMN, nlog::OpCode::ALTER_COLUMN},
       {ASTAlterCommand::Type::RENAME_COLUMN, nlog::OpCode::ALTER_COLUMN},
       {ASTAlterCommand::Type::MODIFY_TTL, nlog::OpCode::ALTER_TABLE},
       {ASTAlterCommand::Type::DROP_COLUMN, nlog::OpCode::DELETE_COLUMN}};

nlog::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter)
{
    if (alter.command_list->children.empty())
        return nlog::OpCode::MAX_OPS_CODE;

    for (const auto & child : alter.command_list->children)
    {
        if (auto * cmd = child->as<ASTAlterCommand>())
        {
            auto iter = command_type_to_opCode.find(cmd->type);
            if (iter != command_type_to_opCode.end())
            {
                return iter->second;
            }
        }
    }
    return nlog::OpCode::MAX_OPS_CODE;
}

String getJSONFromCreateQuery(const ASTCreateQuery & create)
{
    assert(create.storage);

    String payload_str;
    Poco::JSON::Object payload;
    UInt64 shards = create.storage->engine->arguments->children[0]->as<ASTLiteral &>().value.safeGet<UInt64>();
    UInt64 replicas = create.storage->engine->arguments->children[1]->as<ASTLiteral &>().value.safeGet<UInt64>();
    String shard_by_expression = queryToString(create.storage->engine->arguments->children[2]);

    Field mode("");
    if (create.storage->settings)
        create.storage->settings->changes.tryGet("mode", mode);

    Poco::JSON::Object table_mapping_json;
    payload.set("name", create.getTable());
    payload.set("shards", shards);
    payload.set("replication_factor", replicas);
    payload.set("shard_by_expression", shard_by_expression);

    /// If primary key / order by / partition key are specified, honor them
    if (create.storage->primary_key)
        payload.set("primary_key", queryToString(*create.storage->primary_key));

    if (create.storage->order_by)
        payload.set("order_by_expression", queryToString(*create.storage->order_by));
    else
        payload.set("order_by_granularity", "H");

    if (create.storage->partition_by)
        payload.set("partition_by_expression", queryToString(*create.storage->partition_by));
    else
        payload.set("partition_by_granularity", "D");

    if (mode == ProtonConsts::CHANGELOG_KV_MODE || mode == ProtonConsts::VERSIONED_KV_MODE)
    {
        assert(create.storage->primary_key);
        payload.set("mode", mode.get<String>());
    }

    if (create.uuid != UUIDHelpers::Nil)
        payload.set("uuid", toString(create.uuid));

    if (create.storage->ttl_table)
        payload.set("ttl_expression", queryToString(*create.storage->ttl_table));

    buildColumnsJSON(payload, create.columns_list);

    return JSONToString(payload);
}

String getJSONFromAlterQuery(const ASTAlterQuery & alter)
{
    String payload;
    if (alter.command_list->children.empty())
        return payload;

    bool has_payload = false;
    Poco::JSON::Object payload_json;

    for (const auto & child : alter.command_list->children)
    {
        if (auto * cmd = child->as<ASTAlterCommand>())
        {
            if (cmd->type == ASTAlterCommand::Type::ADD_COLUMN || cmd->type == ASTAlterCommand::Type::MODIFY_COLUMN)
            {
                const auto & column = cmd->col_decl->as<ASTColumnDeclaration &>();
                if (!column.name.starts_with("_tp_"))
                {
                    ColumnDeclarationToJSON(payload_json, column);
                    has_payload = true;
                }
            }
            else if (cmd->type == ASTAlterCommand::Type::DROP_COLUMN)
            {
                String col = queryToString(cmd->column);
                if (!col.starts_with("_tp_"))
                {
                    payload_json.set("name", col);
                    has_payload = true;
                }
            }
            else if (cmd->type == ASTAlterCommand::Type::RENAME_COLUMN)
            {
                String col = queryToString(cmd->rename_to);
                if (!col.starts_with("_tp_"))
                {
                    payload_json.set("name", col);
                    has_payload = true;
                }
            }
            else if (cmd->type == ASTAlterCommand::Type::MODIFY_TTL)
            {
                payload_json.set("ttl_expression", queryToString(cmd->ttl));
                has_payload = true;
            }
        }
    }

    if (has_payload)
        payload = JSONToString(payload_json);

    return payload;
}

void waitForDDLOps(Poco::Logger * log, const ContextMutablePtr & ctx, bool force_sync, UInt64 timeout)
{
    UInt64 wait_time = 0;
    if (!ctx->getSettingsRef().synchronous_ddl && !force_sync)
        return;

    /// FIXME: shall route to task service in a distributed env if task service is not local
    auto & task_service = TaskStatusService::instance(ctx);
    while (true)
    {
        /// Loop wait to finish, sleep interval too large?
        /// Actually, the create delay is about '1s'
        LOG_INFO(log, "Wait for DDL operation for query_id={} ...", ctx->getCurrentQueryId());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        wait_time += 100;

        auto task_status = task_service.findById(ctx->getCurrentQueryId());
        if (task_status)
        {
            if (task_status->status == TaskStatusService::TaskStatus::SUCCEEDED)
                break;
            else if (task_status->status == TaskStatusService::TaskStatus::FAILED)
                throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Fail to do DDL. reason: {}", task_status->reason);
        }

        if (wait_time > timeout)
        {
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Wait {} milliseconds for DDL operation timeout. the timeout is {} milliseconds.",
                wait_time,
                timeout);
        }
    }
}
}
