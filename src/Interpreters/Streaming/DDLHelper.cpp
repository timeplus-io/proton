#include "DDLHelper.h"

#include <Common/ProtonCommon.h>
#include "ASTToJSONUtils.h"

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

ASTPtr functionToAST(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserFunction parser;
    return parseQuery(parser, start, end, "", 0, 0);
}

void prepareEngine(ASTCreateQuery & create)
{
    Field shards = DEFAULT_SHARDS;
    Field replicas = DEFAULT_REPLICAS;
    Field sharding_expr_field = DEFAULT_SHARDING_EXPR;
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

    auto engine = makeASTFunction(
        "DistributedMergeTree", std::make_shared<ASTLiteral>(shards), std::make_shared<ASTLiteral>(replicas), sharding_expr);
    create.storage->set(create.storage->engine, engine);
}

void prepareColumns(ASTCreateQuery & create)
{
    const ASTs & column_asts = create.columns_list->columns->children;
    auto new_columns = std::make_shared<ASTExpressionList>();

    Field event_time_default = DEFAULT_EVENT_TIME;

    if (create.storage->settings && !create.storage->settings->changes.empty())
    {
        create.storage->settings->changes.tryGet("event_time_column", event_time_default);
    }
    String expr;
    event_time_default.tryGet<String>(expr);

    bool has_event_time = false;
    bool has_index_time = false;
    for (const ASTPtr & column_ast : column_asts)
    {
        const auto & column = column_ast->as<ASTColumnDeclaration &>();

        /// Skip reserved internal columns
        if (column.name.starts_with("_tp_"))
        {
            if (RESERVED_EVENT_TIME == column.name)
            {
                has_event_time = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "DateTime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'DateTime64' but actual type '{}'.",
                        RESERVED_EVENT_TIME,
                        column.type->getID());
            }
            else if (RESERVED_INDEX_TIME == column.name)
            {
                has_index_time = true;
                auto type_name = tryGetFunctionName(column.type);
                if (!type_name || *type_name != "DateTime64")
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Column {} is reserved, expected type 'DateTime64' but actual type '{}'.",
                        RESERVED_INDEX_TIME,
                        column.type->getID());
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column {} is reserved, should not used in create query.", column.name);
        }

        new_columns->children.emplace_back(column_ast);
    }

    if (!has_event_time)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = RESERVED_EVENT_TIME;
        col_tp_time->type
            = makeASTFunction("DateTime64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));
        col_tp_time->default_specifier = "DEFAULT";
        col_tp_time->default_expression = functionToAST(expr);
        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        auto func_double_delta = std::make_shared<ASTFunction>();
        func_double_delta->name = "DoubleDelta";
        auto func_lz4 = std::make_shared<ASTFunction>();
        func_lz4->name = "LZ4";
        col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_lz4));
        new_columns->children.emplace_back(col_tp_time);
    }

    if (!has_index_time)
    {
        auto col_tp_time = std::make_shared<ASTColumnDeclaration>();
        col_tp_time->name = RESERVED_INDEX_TIME;
        col_tp_time->type
            = makeASTFunction("DateTime64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));
        col_tp_time->default_specifier = "DEFAULT";
        col_tp_time->default_expression
            = makeASTFunction("now64", std::make_shared<ASTLiteral>(Field(UInt64(3))), std::make_shared<ASTLiteral>("UTC"));
        /// makeASTFunction cannot be used because 'DoubleDelta' and 'LZ4' need null arguments.
        auto func_double_delta = std::make_shared<ASTFunction>();
        func_double_delta->name = "DoubleDelta";
        auto func_lz4 = std::make_shared<ASTFunction>();
        func_lz4->name = "LZ4";
        col_tp_time->codec = makeASTFunction("CODEC", std::move(func_double_delta), std::move(func_lz4));
        new_columns->children.emplace_back(col_tp_time);
    }

    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, new_columns);

    create.replace(create.columns_list, new_columns_list);
    /// as columns_list has been replaced, the existing pointer create.storage->primary_key becomes invalid, therefore set it to nullptr
    create.storage->primary_key = nullptr;
}

void prepareOrderByAndPartitionBy(ASTCreateQuery & create)
{
    /// FIXME: raw table might have different order by and partition by
    auto new_order_by = makeASTFunction("toStartOfHour", std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
    create.storage->set(create.storage->order_by, new_order_by);

    auto new_partition_by = makeASTFunction("toYYYYMMDD", std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
    create.storage->set(create.storage->partition_by, new_partition_by);
}

void prepareCreateQueryForDistributedMergeTree(ASTCreateQuery & create)
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

DWAL::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams)
{
    if (queryParams.contains("column"))
    {
        auto iter = queryParams.find("query_method");

        if (iter->second == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return DWAL::OpCode::CREATE_COLUMN;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return DWAL::OpCode::ALTER_COLUMN;
        }
        else if (iter->second == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return DWAL::OpCode::DELETE_COLUMN;
        }
        else
        {
            assert(false);
            return DWAL::OpCode::UNKNOWN;
        }
    }

    return DWAL::OpCode::ALTER_TABLE;
}

std::map<ASTAlterCommand::Type, DWAL::OpCode> command_type_to_opCode
    = {{ASTAlterCommand::Type::ADD_COLUMN, DWAL::OpCode::CREATE_COLUMN},
       {ASTAlterCommand::Type::MODIFY_COLUMN, DWAL::OpCode::ALTER_COLUMN},
       {ASTAlterCommand::Type::RENAME_COLUMN, DWAL::OpCode::ALTER_COLUMN},
       {ASTAlterCommand::Type::MODIFY_TTL, DWAL::OpCode::ALTER_TABLE},
       {ASTAlterCommand::Type::DROP_COLUMN, DWAL::OpCode::DELETE_COLUMN}};

DWAL::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter)
{
    if (alter.command_list->children.empty())
        return DWAL::OpCode::UNKNOWN;

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
    return DWAL::OpCode::UNKNOWN;
}

String getJSONFromCreateQuery(const ASTCreateQuery & create)
{
    String payload_str;
    Poco::JSON::Object payload;
    UInt64 shards = create.storage->engine->arguments->children[0]->as<ASTLiteral &>().value.safeGet<UInt64>();
    UInt64 replicas = create.storage->engine->arguments->children[1]->as<ASTLiteral &>().value.safeGet<UInt64>();
    String shard_by_expression = queryToString(create.storage->engine->arguments->children[2]);

    Poco::JSON::Object table_mapping_json;
    payload.set("name", create.getTable());
    payload.set("shards", shards);
    payload.set("replication_factor", replicas);
    payload.set("shard_by_expression", shard_by_expression);
    /// FIXME: parse order by expression and partition by expression
    payload.set("order_by_granularity", "H");
    payload.set("partition_by_granularity", "D");

    if (create.uuid != UUIDHelpers::Nil)
        payload.set("uuid", toString(create.uuid));

    if (create.storage && create.storage->ttl_table)
    {
        payload.set("ttl", queryToString(*create.storage->ttl_table));
    }
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
    {
        payload = JSONToString(payload_json);
    }
    return payload;
}
}
