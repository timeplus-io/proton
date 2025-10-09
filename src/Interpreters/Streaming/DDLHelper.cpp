#include <Interpreters/Streaming/ASTToJSONUtils.h>
#include <Interpreters/Streaming/DDLHelper.h>

#include <Cluster/KafkaLog/KafkaWALPool.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/extractKeyExpressionList.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/logger_useful.h>

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

extern const int OK;
extern const int RESOURCE_ALREADY_EXISTS;
extern const int DWAL_FATAL_ERROR;
extern const int INVALID_LOGSTORE_REPLICATION_FACTOR;
extern const int RESOURCE_NOT_FOUND;
}

namespace Streaming
{
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
    "ingest_mode",
    "flush_threshold_ms",
    "flush_threshold_count",
    "flush_threshold_bytes",
    "storage_type",
    "logstore",
    "mode",
};

void normalizeColumns(ASTCreateQuery & create, ContextPtr context)
{
    if (create.columns_list)
        return;

    create.set(create.columns_list, std::make_shared<ASTColumns>());
    if (!create.as_table.empty())
    {
        /// Case: `create stream t1 as test`
        String as_database_name = context->resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, context);

        /// as_storage->getColumns() must be called under structure lock of other_table for CREATE ... AS other_table.
        TableLockHolder as_storage_lock
            = as_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
        create.columns_list->setOrReplace(
            create.columns_list->columns, InterpreterCreateQuery::formatColumns(as_storage->getInMemoryMetadataPtr()->getColumns()));
    }
    else if (create.select)
    {
        /// Case: `create stream t1 as select * from test`
        Block as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), context, false);
        create.columns_list->setOrReplace(
            create.columns_list->columns, InterpreterCreateQuery::formatColumns(as_select_sample.getNamesAndTypesList()));
    }
    else if (create.as_table_function)
    {
        /// Case: `create stream t1 as dedup(test, i)`
        /// Table function without columns list.
        auto table_function = TableFunctionFactory::instance().get(create.as_table_function, context);
        create.columns_list->setOrReplace(
            create.columns_list->columns, InterpreterCreateQuery::formatColumns(table_function->getActualTableStructure(context)));
    }
}
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
        else if (key == "ingest_mode")
        {
            if (value != "async" && value != "sync")
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "ingest_mode only supports 'async' or 'sync'");
        }
        else if (key == "storage_type")
        {
            if (value != "hybrid" && value != "streaming" && value != "memory")
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "storage_type only supports 'hybrid' or 'streaming' or 'memory'");
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

void prepareStorageEngine(ASTCreateQuery & create, ContextPtr ctx)
{
    Field shards = ctx->getStreamSettings().default_shards.value;
    Field sharding_expr_field = ctx->getStreamSettings().default_sharding_expr.value;

    String expr;

    if (!create.storage)
    {
        create.set(create.storage, std::make_shared<ASTStorage>());
    }
    else if (create.storage->settings && !create.storage->settings->changes.empty())
    {
        create.storage->settings->changes.tryGet("shards", shards);
        create.storage->settings->changes.tryGet("sharding_expr", sharding_expr_field);
    }

    sharding_expr_field.tryGet<String>(expr);

    ASTPtr sharding_expr;
    if (expr.empty())
    {
        /// Default sharding expr:
        /// 1) If has primary keys, default is `weak_hash32(<primary_keys>)`
        /// 2) Otherwise, default is `rand()`
        if (create.storage->primary_key)
            sharding_expr = makeASTFunction("weak_hash32", create.storage->primary_key->clone());
        else
            sharding_expr = makeASTFunction("rand");
    }
    else
        sharding_expr = functionToAST(expr);

    auto engine
        = makeASTFunction("Stream", std::make_shared<ASTLiteral>(shards), sharding_expr);
    create.storage->set(create.storage->engine, engine);
}

void prepareStorageEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx)
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

void checkAndPrepareColumnsAndIndexes(ASTCreateQuery & create, ContextPtr context)
{
    /// Set and retrieve list of columns
    normalizeColumns(create, context);

    /// columns_list should contains valid column definition
    if (!create.columns_list || !create.columns_list->columns || create.columns_list->columns->children.empty())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Columns is empty no column has been defined.");

    ASTs & column_asts = create.columns_list->columns->children;
    Field event_time_default = ProtonConsts::DEFAULT_EVENT_TIME;

    if (create.storage && create.storage->settings && !create.storage->settings->changes.empty())
        create.storage->settings->changes.tryGet("event_time_column", event_time_default);

    String event_time_default_expr;
    event_time_default.tryGet<String>(event_time_default_expr);

    bool has_event_time = false;
    bool has_event_time_index = false;
    [[maybe_unused]] bool has_index_time = false;
    bool has_sequence_id = false;
    bool has_event_sn_index = false;
    bool has_delta_flag = false;
    String auto_increment_column_name;

    for (const ASTPtr & column_ast : column_asts)
    {
        auto & column = column_ast->as<ASTColumnDeclaration &>();

        if (boost::iequals(column.default_specifier, "AUTO_INCREMENT"))
            auto_increment_column_name = column.name;

        /// Skip reserved internal columns
        if ((column.name.starts_with("_tp_") && column.name != ProtonConsts::RESERVED_MESSAGE_KEY)
            || std::find(
                   ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.begin(), ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end(), column.name)
                != ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end())
        {
            if (ProtonConsts::RESERVED_EVENT_TIME == column.name)
            {
                /// Alias
                if (!column.type)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Reserved column {} can't be alias", column.name);

                auto * type = column.type->as<ASTDataType>();
                if (type == nullptr || type->name != "datetime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'datetime64' but actual type '{}'.",
                        ProtonConsts::RESERVED_EVENT_TIME,
                        column.type->getID());

                has_event_time = true;
            }
            else if (ProtonConsts::RESERVED_INDEX_TIME == column.name)
            {
                /// Alias
                if (!column.type)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Reserved column {} can't be alias", column.name);

                auto * type = column.type->as<ASTDataType>();
                if (type == nullptr || type->name != "datetime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'datetime64' but actual type '{}'.",
                        column.name,
                        column.type->getID());

                has_index_time = true;
            }
            else if (ProtonConsts::RESERVED_EVENT_SEQUENCE_ID == column.name)
            {
                /// Alias
                if (!column.type)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Reserved column {} can't be alias", column.name);

                auto * type = column.type->as<ASTDataType>();
                if (type == nullptr || type->name != "int64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'int64 ' but actual type '{}'.",
                        ProtonConsts::RESERVED_EVENT_SEQUENCE_ID,
                        column.type->getID());

                has_sequence_id = true;
            }
            else if (ProtonConsts::RESERVED_DELTA_FLAG == column.name)
            {
                /// Alias
                if (!column.type)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Reserved column {} can't be alias", column.name);

                auto * type = column.type->as<ASTDataType>();
                if (type == nullptr || type->name != "int8")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'int8' but actual type '{}'.",
                        ProtonConsts::RESERVED_DELTA_FLAG,
                        column.type->getID());

                has_delta_flag = true;
            }
            else
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column {} is reserved, should not used in create query.", column.name);
            }
        }
    }

    if (create.columns_list->indices)
    {
        for (const ASTPtr & column_index : create.columns_list->indices->children)
        {
            const auto & index = column_index->as<ASTIndexDeclaration &>();

            if (ProtonConsts::RESERVED_EVENT_TIME_INDEX == index.name)
                has_event_time_index = true;
            else if (ProtonConsts::RESERVED_EVENT_SEQUENCE_INDEX == index.name)
                has_event_sn_index = true;
        }
    }

    if (!has_event_time)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = ProtonConsts::RESERVED_EVENT_TIME;
        col_tp_time->type
            = makeASTDataType("datetime64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));

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
        if (!create.isMaterializedView())
        {
            auto func_double_delta = std::make_shared<ASTFunction>();
            func_double_delta->name = "DoubleDelta";
            auto func_compression = std::make_shared<ASTFunction>();
            func_compression->name = "ZSTD";
            col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_compression));
        }

        column_asts.emplace_back(std::move(col_tp_time));
    }

    if (!has_event_time_index && !create.isMaterializedView())
    {
        auto expr = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME);
        auto type = std::make_shared<ASTFunction>();
        type->name = "minmax";
        type->no_empty_args = true;
        auto index = std::make_shared<ASTIndexDeclaration>();
        index->name = ProtonConsts::RESERVED_EVENT_TIME_INDEX;
        index->granularity = 32;
        index->set(index->expr, expr);
        index->set(index->type, type);
        if (create.columns_list->indices == nullptr)
            create.columns_list->set(create.columns_list->indices, std::make_shared<DB::ASTExpressionList>());
        create.columns_list->indices->children.emplace_back(std::move(index));
    }

#if 0
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
        if (!create.is_kv && !create.is_materialized_view)
        {
            auto func_double_delta = std::make_shared<ASTFunction>();
            func_double_delta->name = "DoubleDelta";
            auto func_lz4 = std::make_shared<ASTFunction>();
            func_lz4->name = "LZ4";
            col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_lz4));
            column_asts.emplace_back(std::move(col_tp_time));
        }
    }
#endif

    if (!has_sequence_id)
    {
        auto col_tp_sn = std::make_shared<ASTColumnDeclaration>();
        col_tp_sn->name = ProtonConsts::RESERVED_EVENT_SEQUENCE_ID;
        col_tp_sn->type = makeASTDataType("int64");

        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        if (!create.isMaterializedView())
        {
            auto func_delta = std::make_shared<ASTFunction>();
            func_delta->name = "Delta";
            auto func_compression = std::make_shared<ASTFunction>();
            func_compression->name = "ZSTD";
            col_tp_sn->codec = makeASTFunction("CODEC", std::move(func_delta), std::move(func_compression));
        }

        column_asts.emplace_back(std::move(col_tp_sn));
    }

    if (!has_event_sn_index && !create.isMaterializedView())
    {
        auto expr = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
        auto type = std::make_shared<ASTFunction>();
        type->name = "minmax";
        type->no_empty_args = true;
        auto index = std::make_shared<ASTIndexDeclaration>();
        index->name = ProtonConsts::RESERVED_EVENT_SEQUENCE_INDEX;
        index->granularity = 32;
        index->set(index->expr, expr);
        index->set(index->type, type);
        if (create.columns_list->indices == nullptr)
            create.columns_list->set(create.columns_list->indices, std::make_shared<DB::ASTExpressionList>());
        create.columns_list->indices->children.emplace_back(std::move(index));
    }

    /// Only changelog stream needs delta flag
    if (!has_delta_flag)
    {
        Field mode("");
        if (create.storage && create.storage->settings)
            create.storage->settings->changes.tryGet("mode", mode);

        if (mode == ProtonConsts::CHANGELOG_MODE || mode == ProtonConsts::CHANGELOG_KV_MODE)
        {
            auto delta_flag = std::make_shared<ASTColumnDeclaration>();
            delta_flag->name = ProtonConsts::RESERVED_DELTA_FLAG;
            delta_flag->type = makeASTDataType("int8");
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

        /// versioned-kv and changelog-kv only supports partition by primary key
        /// if (create.storage->partition_by)
        ///    throw Exception(ErrorCodes::SYNTAX_ERROR, "Primary key is required for changelog kv or versioned kv stream");
    }
    else
    {
        if (!create.storage->order_by && !create.storage->primary_key)
        {
            auto new_order_by = makeASTFunction("to_start_of_hour", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
            create.storage->set(create.storage->order_by, new_order_by);
        }

        if (!create.storage->partition_by)
        {
            auto new_partition_by = makeASTFunction("to_YYYYMM", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
            create.storage->set(create.storage->partition_by, new_partition_by);
        }
    }
}

void checkAndPrepareCreateQueryForStream(ASTCreateQuery & create, ContextPtr context)
{
    checkAndPrepareColumnsAndIndexes(create, context);
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
        columnDeclarationToJSON(column_mapping_json, col_decl);
        columns_mapping_json.add(column_mapping_json);
    }
    resp_table.set("columns", columns_mapping_json);
}

cluster::protocol::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams)
{
    if (queryParams.contains("column"))
    {
        auto iter = queryParams.find("query_method");

        if (iter->second == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return cluster::protocol::OpCode::CreateColumn;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return cluster::protocol::OpCode::AlterColumn;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return cluster::protocol::OpCode::DeleteColumn;
        }
        else
        {
            assert(false);
            return cluster::protocol::OpCode::Null;
        }
    }

    return cluster::protocol::OpCode::AlterStreamSettings;
}

const std::map<ASTAlterCommand::Type, cluster::protocol::OpCode> command_type_to_opcode
    = {{ASTAlterCommand::Type::ADD_COLUMN, cluster::protocol::OpCode::CreateColumn},
       {ASTAlterCommand::Type::MODIFY_COLUMN, cluster::protocol::OpCode::AlterColumn},
       {ASTAlterCommand::Type::RENAME_COLUMN, cluster::protocol::OpCode::AlterColumn},
       {ASTAlterCommand::Type::MODIFY_TTL, cluster::protocol::OpCode::AlterStreamSettings},
       {ASTAlterCommand::Type::MODIFY_SETTING, cluster::protocol::OpCode::AlterStreamSettings},
       {ASTAlterCommand::Type::DROP_COLUMN, cluster::protocol::OpCode::DeleteColumn}};

cluster::protocol::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter)
{
    if (alter.command_list->children.empty())
        return cluster::protocol::OpCode::Null;

    for (const auto & child : alter.command_list->children)
    {
        if (auto * cmd = child->as<ASTAlterCommand>())
        {
            auto iter = command_type_to_opcode.find(cmd->type);
            if (iter != command_type_to_opcode.end())
            {
                return iter->second;
            }
        }
    }
    return cluster::protocol::OpCode::Null;
}

String getJSONFromCreateQuery(const ASTCreateQuery & create)
{
    assert(create.storage);

    String payload_str;
    Poco::JSON::Object payload;
    UInt64 shards = create.storage->engine->arguments->children[0]->as<ASTLiteral &>().value.safeGet<UInt64>();
    String shard_by_expression = queryToString(create.storage->engine->arguments->children[1]);

    Field mode("");
    if (create.storage->settings)
        create.storage->settings->changes.tryGet("mode", mode);

    Poco::JSON::Object table_mapping_json;
    payload.set("name", create.getTable());
    payload.set("shards", shards);
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

    if (mode == ProtonConsts::CHANGELOG_KV_MODE || mode == ProtonConsts::VERSIONED_KV_MODE || mode == ProtonConsts::CHANGELOG_MODE)
        payload.set("mode", mode.get<String>());

    if (create.uuid != UUIDHelpers::Nil)
        payload.set("uuid", toString(create.uuid));

    if (create.storage->ttl_table)
        payload.set("ttl_expression", queryToString(*create.storage->ttl_table));

    buildColumnsJSON(payload, create.columns_list);

    return jsonToString(payload);
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
                    columnDeclarationToJSON(payload_json, column);
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
            else if (cmd->type == ASTAlterCommand::Type::MODIFY_SETTING)
            {
                settingsToJSON(payload_json, cmd->settings_changes->as<ASTSetQuery &>().changes);
                if (payload_json.size() > 0)
                    has_payload = true;
            }
        }
    }

    if (has_payload)
        payload = jsonToString(payload_json);

    return payload;
}

TTLSettings parseTTLSettings(const String & payload)
{
    std::vector<std::pair<String, String>> stream_settings;
    String ttl_expr;
    Poco::JSON::Parser parser;
    auto json = parser.parse(payload).extract<Poco::JSON::Object::Ptr>();

    for (const auto & [k, v] : ProtonConsts::LOG_STORE_SETTING_NAME_TO_KAFKA)
        if (json->has(k))
            stream_settings.emplace_back(v, json->get(k).toString());

    if (json->has("ttl_expression"))
        ttl_expr = json->get("ttl_expression").toString();

    return {ttl_expr, stream_settings};
}

bool assertColumnExists(const String & database, const String & table, const String & column, ContextPtr ctx)
{
    auto storage = DatabaseCatalog::instance().tryGetTable({database, table}, ctx);
    if (!storage)
        return false;

    auto column_names_and_types{storage->getInMemoryMetadata().getColumns().getOrdinary()};
    if (column_names_and_types.contains(column))
        return true;

    return false;
}

#if 0 /// FIXME, cluster
void waitUntilDWalReady(const klog::KafkaWALContext & ctx, ContextPtr global_context)
{
    klog::KafkaWALPtr dwal = klog::KafkaWALPool::instance(global_context).getMeta();
    auto log = getLogger("DDLHelper");
    while (true)
    {
        if (dwal->describe(ctx.topic).err == ErrorCodes::OK)
        {
            return;
        }
        else
        {
            LOG_INFO(log, "Wait for topic={} to be ready...", ctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void doCreateDWal(const klog::KafkaWALContext & ctx, ContextPtr global_context)
{
    klog::KafkaWALPtr dwal = klog::KafkaWALPool::instance(global_context).getMeta();
    auto log = getLogger("DDLHelper");

    if (dwal->describe(ctx.topic).err == ErrorCodes::OK)
    {
        LOG_INFO(log, "Found topic={} already exists", ctx.topic);
        return;
    }

    LOG_INFO(log, "Didn't find topic={}, create one with settings={}", ctx.topic, ctx.string());

    constexpr int32 DWAL_MAX_RETRIES = 3;
    int32 retries = 0;
    while (true)
    {
        auto err = dwal->create(ctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            LOG_INFO(log, "Successfully created topic={}", ctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_ALREADY_EXISTS)
        {
            LOG_INFO(log, "Topic={} already exists", ctx.topic);
            break;
        }
        else if (err == ErrorCodes::DWAL_FATAL_ERROR)
        {
            throw Exception("Underlying streaming store " + ctx.topic + " create failed due to fatal error.", ErrorCodes::DWAL_FATAL_ERROR);
        }
        else if (err == ErrorCodes::INVALID_LOGSTORE_REPLICATION_FACTOR)
        {
            throw Exception(
                "Underlying streaming store " + ctx.topic + " create failed due to invalid replication factor.",
                ErrorCodes::INVALID_LOGSTORE_REPLICATION_FACTOR);
        }
        else
        {
            if (retries >= DWAL_MAX_RETRIES)
            {
                throw Exception("Underlying streaming store " + ctx.topic + " create failed", ErrorCodes::UNKNOWN_EXCEPTION);
            }

            retries++;
            LOG_INFO(log, "Failed to create topic={} and will try to create it={} again...", ctx.topic, retries);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    waitUntilDWalReady(ctx, global_context);
}

void doDeleteDWal(const klog::KafkaWALContext & ctx, ContextPtr global_context)
{
    auto dwal = Globals
    klog::KafkaWALPtr dwal = klog::KafkaWALPool::instance(global_context).getMeta();
    auto log = getLogger("DDLHelper");

    int retries = 0;
    while (retries++ < DWAL_MAX_RETRIES)
    {
        auto err = dwal->remove(ctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully deleted topic={}", ctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_NOT_FOUND)
        {
            LOG_INFO(log, "Topic={} not exists", ctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to delete topic={}, will retry ...", ctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void deleteDWAL(const String & uuid, ContextPtr global_context)
{
    klog::KafkaWALContext ctx{uuid};
    doDeleteDWal(ctx, global_context);
}
#endif

int extractErrorCodeFromMsg(const String & err_msg)
{
    if (auto pos = err_msg.find("Code: "); pos != String::npos)
        return std::atoi(err_msg.c_str() + pos + 6);
    else if (auto pos2 = err_msg.find("code:"); pos2 != String::npos)
        return std::atoi(err_msg.c_str() + pos2 + 5);
    else
        return 0;
}
}
}
