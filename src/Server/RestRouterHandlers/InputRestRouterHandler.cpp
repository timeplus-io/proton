#include <Server/RestRouterHandlers/InputRestRouterHandler.h>

#include <Server/RestRouterHandlers/queryStreams.h>

#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InputSettingsUtils.h>
#include <Interpreters/InputTargetStreams.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/FieldVisitorToString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <fmt/format.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_STREAM;
}

namespace
{

bool isSensitiveSettingName(const String & name)
{
    String lower = name;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](char c) { return isAlphaASCII(c) ? toLowerIfAlphaASCII(c) : c; });

    static const std::array<std::string_view, 18> sensitive_substrings = {
        "password",
        "passwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "access_key",
        "secret_key",
        "session_key",
        "private_key",
        "client_secret",
        "oauth",
        "bearer",
        "authorization",
        "sasl_password",
        "ssl_key_password",
        "ssl_private_key",
        "credentials",
    };

    for (const auto needle : sensitive_substrings)
    {
        if (lower.find(needle) != String::npos)
            return true;
    }
    return false;
}

Poco::Dynamic::Var fieldToJSONValue(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Null:
            return Poco::Dynamic::Var();
        case Field::Types::String:
            return field.safeGet<String>();
        case Field::Types::UInt64:
            return Poco::UInt64(field.safeGet<UInt64>());
        case Field::Types::Int64:
            return Poco::Int64(field.safeGet<Int64>());
        case Field::Types::Float64:
            return field.safeGet<Float64>();
        case Field::Types::Bool:
            return field.safeGet<bool>();
        default:
            return toString(field);
    }
}

bool equalsCaseInsensitiveString(std::string_view lhs, std::string_view rhs)
{
    if (lhs.size() != rhs.size())
        return false;

    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](char a, char b) { return equalsCaseInsensitive(a, b); });
}

void settingsToJSON(Poco::JSON::Object & settings_json, const ASTSetQuery * settings_ast)
{
    if (!settings_ast)
        return;

    for (const auto & change : settings_ast->changes)
    {
        if (isSensitiveSettingName(change.name))
            settings_json.set(change.name, "******");
        else
            settings_json.set(change.name, fieldToJSONValue(change.value));
    }
}

String getStringSettingValue(const ASTSetQuery * settings_ast, std::string_view key)
{
    if (!settings_ast)
        return {};

    for (const auto & change : settings_ast->changes)
    {
        if (!equalsCaseInsensitiveString(change.name, key))
            continue;
        if (change.value.getType() != Field::Types::String)
            return {};
        return change.value.safeGet<String>();
    }

    return {};
}

const ASTSetQuery * tryParseSettingsClause(ContextMutablePtr query_context, const String & create_table_query, ASTPtr & ast_holder)
{
    if (!query_context || create_table_query.empty())
        return nullptr;

    static constexpr std::string_view keyword = "SETTINGS";
    auto it = std::search(create_table_query.begin(), create_table_query.end(), keyword.begin(), keyword.end(), [](char a, char b) {
        return equalsCaseInsensitive(a, b);
    });

    if (it == create_table_query.end())
        return nullptr;

    const char * begin = create_table_query.data() + (it - create_table_query.begin()) + keyword.size();
    const char * end = create_table_query.data() + create_table_query.size();

    ParserSetQuery parser(/*parse_only_internals=*/true);
    const auto & settings = query_context->getSettingsRef();

    try
    {
        ast_holder = parseQuery(parser, begin, end, "SETTINGS clause", settings.max_query_size, settings.max_parser_depth);
    }
    catch (...)
    {
        return nullptr;
    }

    return ast_holder ? ast_holder->as<ASTSetQuery>() : nullptr;
}

String stripNullableType(const String & type, bool & is_nullable)
{
    static constexpr std::string_view prefix = "Nullable(";
    is_nullable = false;

    if (type.size() >= prefix.size() + 1 && type.starts_with(prefix) && type.ends_with(')'))
    {
        is_nullable = true;
        return type.substr(prefix.size(), type.size() - prefix.size() - 1);
    }
    return type;
}

void buildColumnsJSONFromSystemColumns(
    ContextMutablePtr query_context, Poco::JSON::Object & resp_table, const String & database, const String & table)
{
    Poco::JSON::Array columns_json;

    String query = fmt::format(
        "SELECT name, type, default_kind, default_expression, comment FROM system.columns "
        "WHERE database = {} AND table = {} ORDER BY position SETTINGS _tp_internal_system_open_sesame=true",
        quoteString(database),
        quoteString(table));

    executeNonInsertQuery(query, query_context, [&](Block && block) {
        for (size_t row = 0; row < block.rows(); ++row)
        {
            String name;
            String type;
            String default_kind;
            String default_expression;
            String comment;

            for (const auto & col : block)
            {
                if (col.name == "name")
                    name = col.column->getDataAt(row).toString();
                else if (col.name == "type")
                    type = col.column->getDataAt(row).toString();
                else if (col.name == "default_kind")
                    default_kind = col.column->getDataAt(row).toString();
                else if (col.name == "default_expression")
                    default_expression = col.column->getDataAt(row).toString();
                else if (col.name == "comment")
                    comment = col.column->getDataAt(row).toString();
            }

            Poco::JSON::Object col_json;
            col_json.set("name", name);

            bool is_nullable = false;
            String stripped_type = stripNullableType(type, is_nullable);
            col_json.set("nullable", is_nullable);
            col_json.set("type", stripped_type);

            if (default_kind == "DEFAULT" && !default_expression.empty())
                col_json.set("default", default_expression);
            else if (default_kind == "ALIAS" && !default_expression.empty())
                col_json.set("alias", default_expression);

            if (!comment.empty())
                col_json.set("comment", comment);

            columns_json.add(col_json);
        }
    });

    resp_table.set("columns", columns_json);
}

}

std::pair<String, Int32> InputRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    String requested_database = getPathParameter("database", database);
    const String requested_name = getPathParameter("input");

    if (requested_database.empty())
        requested_database = database;

    if (!DatabaseCatalog::instance().tryGetDatabase(requested_database))
        return {
            jsonErrorResponse(fmt::format("Database '{}' does not exist.", requested_database), ErrorCodes::UNKNOWN_DATABASE),
            HTTPResponse::HTTP_NOT_FOUND};

    TablePtrs tables;
    const auto node_identity{query_context->getNodeUUID()};
    const auto this_host{query_context->getHostFQDN()};

    if (requested_name.empty())
    {
        queryStreamsByDatabase(query_context, requested_database, [&](Block && block) {
            tables.reserve(block.rows());
            for (size_t row = 0; row < block.rows(); ++row)
                tables.push_back(std::make_shared<Table>(DB::toString(node_identity), this_host, block, row));
        });
    }
    else
    {
        queryOneStream(query_context, requested_database, requested_name, [&](Block && block) {
            tables.reserve(block.rows());
            for (size_t row = 0; row < block.rows(); ++row)
                tables.push_back(std::make_shared<Table>(DB::toString(node_identity), this_host, block, row));
        });

        if (tables.empty())
            return {
                jsonErrorResponse(
                    fmt::format("No input named '{}' in database '{}'", requested_name, requested_database), ErrorCodes::UNKNOWN_STREAM),
                HTTPResponse::HTTP_NOT_FOUND};
    }

    TablePtrs inputs;
    inputs.reserve(tables.size());
    for (const auto & table : tables)
        if (table->is_input)
            inputs.push_back(table);

    if (!requested_name.empty() && inputs.empty())
        return {
            jsonErrorResponse(
                fmt::format("No input named '{}' in database '{}'", requested_name, requested_database), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_NOT_FOUND};

    Poco::JSON::Object resp;
    buildTablesJSON(resp, inputs);
    resp.set("request_id", query_context->getCurrentQueryId());

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

void InputRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const
{
    Poco::JSON::Array inputs_json;
    const bool details = !getPathParameter("input").empty();

    for (const auto & table : tables)
    {
        Poco::JSON::Object input_json;
        input_json.set("name", table->name);
        input_json.set("database", table->database);
        input_json.set("engine", table->engine);
        input_json.set("uuid", DB::toString(table->uuid));

        ASTPtr settings_ast_holder;
        const auto * settings_ast = tryParseSettingsClause(query_context, table->create_table_query, settings_ast_holder);

        input_json.set("type", getStringSettingValue(settings_ast, "type"));
        Poco::JSON::Array target_streams_json;

        auto targets = extractTargetStreamValues(settings_ast);
        if (targets.empty())
            targets = tryGetInputTargetStreamsFromRuntime(table->database, table->name, query_context);

        for (const auto & target : targets)
            target_streams_json.add(target);
        input_json.set("target_streams", target_streams_json);

        buildCreatedByAndLastModifiedBy(input_json, table->database, table->name);
        if (!input_json.has("created_by"))
            input_json.set("created_by", "");
        if (!input_json.has("last_modified_by"))
            input_json.set("last_modified_by", "");
        if (!input_json.has("created_at"))
            input_json.set("created_at", Poco::Int64(0));
        if (!input_json.has("last_modified_at"))
            input_json.set("last_modified_at", Poco::Int64(0));

        /// List endpoint: keep response small and avoid exposing config.
        if (!details)
        {
            inputs_json.add(input_json);
            continue;
        }

        Poco::JSON::Object settings_json;
        settingsToJSON(settings_json, settings_ast);
        input_json.set("settings", settings_json);

        buildColumnsJSONFromSystemColumns(query_context, input_json, table->database, table->name);

        inputs_json.add(input_json);
    }

    resp.set("data", inputs_json);
}

}
