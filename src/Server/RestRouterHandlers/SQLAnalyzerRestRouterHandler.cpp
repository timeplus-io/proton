#include <Server/RestRouterHandlers/SQLAnalyzerRestRouterHandler.h>
#include <Server/RestRouterHandlers/SchemaValidator.h>

#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/QueryProfileVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>

#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
/// #include <Parsers/parseQueryPipe.h>
#include <Parsers/parseQuery.h>

#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>

#include <Parsers/Streaming/ASTUnsubscribeQuery.h>
#include <Parsers/Streaming/ASTRecoverQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{
    std::map<String, std::map<String, String>> POST_SCHEMA = {
        {"required", {{"query", "string"}}},
    };

    String buildResponse(
        const String & original_query,
        const String & rewritten_query,
        const String & query_type,
        const QueryProfileMatcher::Data & query_profile,
        const Block & sample_block,
        bool has_aggr,
        std::set<String> group_by_columns,
        bool is_streaming,
        const std::set<std::tuple<String, String, bool, String, String>> & required_columns)
    {
        /// {
        ///    "required_columns": [
        ///        {"database": database, "table": table, "is_view": false, "column": column, "column_type": type},
        ///        ...
        ///    ],
        ///    "result_columns": {
        ///        {"column": column, "column_type": type},
        ///        ...
        ///    },
        ///    "group_by_columns": ["id", "window_start"],
        ///    "rewritten_query": query,
        ///    "original_query": query,
        ///    "query_type": CREATE , SELECT , INSERT INTO , ...
        ///    "has_aggr": true,
        ///    "has_table_join": true,
        ///    "has_union": true,
        ///    "has_subquery": true
        ///    "is_streaming": true,
        /// }

        Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
        result->set("original_query", original_query);
        result->set("rewritten_query", rewritten_query);
        result->set("query_type", query_type);
        result->set("has_aggr", has_aggr);
        result->set("has_table_join", query_profile.has_table_join);
        result->set("has_union", query_profile.has_union);
        result->set("has_subquery", query_profile.has_subquery);
        result->set("is_streaming", is_streaming);

        /// Required columns
        int i = 0;
        Poco::JSON::Array::Ptr required_columns_obj = new Poco::JSON::Array();
        for (auto & columnInfo : required_columns)
        {
            Poco::JSON::Object::Ptr column = new Poco::JSON::Object();
            column->set("database", std::get<0>(columnInfo));
            column->set("table", std::get<1>(columnInfo));
            column->set("is_view", std::get<2>(columnInfo));
            column->set("column", std::get<3>(columnInfo));
            column->set("column_type", std::get<4>(columnInfo));
            required_columns_obj->set(i++, column);
        }
        result->set("required_columns", required_columns_obj);

    /// Group By columns
        Poco::JSON::Array::Ptr group_by_obj = new Poco::JSON::Array();
        for (auto & name : group_by_columns)
            group_by_obj->add(name);

        result->set("group_by_columns", group_by_obj);

        /// Result columns
        i = 0;
        Poco::JSON::Array::Ptr result_columns_obj = new Poco::JSON::Array();
        for (const auto & column_info : sample_block)
        {
            Poco::JSON::Object::Ptr column = new Poco::JSON::Object();
            column->set("column", column_info.name);
            column->set("column_type", column_info.type->getName());
            result_columns_obj->set(i++, column);
        }
        result->set("result_columns", result_columns_obj);

        std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::condense(result, oss);
        return oss.str();
    }

    String queryType(ASTPtr & ast)
    {
        if (ast->as<ASTSelectQuery>())
            return "SELECT";
        else if (ast->as<ASTSelectWithUnionQuery>())
            return "SELECT";
        else if (ast->as<ASTInsertQuery>())
            return "INSERT";
        else if (ast->as<ASTCreateQuery>())
            return "CREATE";
        else if (ast->as<ASTDropQuery>())
            return "DROP";
        else if (ast->as<ASTRenameQuery>())
            return "RENAME";
        else if (ast->as<ASTShowTablesQuery>())
            return "SHOW_TABLE";
        else if (ast->as<ASTUseQuery>())
            return "USE";
        else if (ast->as<ASTSetQuery>())
            return "SET";
        else if (ast->as<ASTSetRoleQuery>())
            return "SET_ROLE";
        else if (ast->as<ASTOptimizeQuery>())
            return "OPTIMIZE";
        else if (ast->as<ASTExistsTableQuery>())
            return "EXISTS_TABLE";
        else if (ast->as<ASTExistsDictionaryQuery>())
            return "EXISTS_DICT";
        else if (ast->as<ASTShowCreateTableQuery>())
            return "SHOW_CREATE_TABLE";
        else if (ast->as<ASTShowCreateDatabaseQuery>())
            return "SHOW_CREATE_DATABASE";
        else if (ast->as<ASTShowCreateDictionaryQuery>())
            return "SHOW_CREATE_DICT";
        else if (ast->as<ASTDescribeQuery>())
            return "DESCRIBE";
        else if (ast->as<ASTExplainQuery>())
            return "EXPLAIN";
        else if (ast->as<ASTShowProcesslistQuery>())
            return "SHOW_PROC_LIST";
        else if (ast->as<ASTAlterQuery>())
            return "ALTER";
        else if (ast->as<ASTCheckQuery>())
            return "CHECK";
        else if (ast->as<ASTKillQueryQuery>())
            return "KILL";
        else if (ast->as<ASTSystemQuery>())
            return "SYSTEM";
        else if (ast->as<ASTWatchQuery>())
            return "WATCH";
        else if (ast->as<ASTCreateUserQuery>())
            return "CREATE_USER";
        else if (ast->as<ASTCreateRoleQuery>())
            return "CREATE_ROLE";
        else if (ast->as<ASTCreateQuotaQuery>())
            return "CREATE_QUOTA";
        else if (ast->as<ASTCreateRowPolicyQuery>())
            return "CREATE_ROW_POLICY";
        else if (ast->as<ASTCreateSettingsProfileQuery>())
            return "CREATE_SETTINGS_PROFILE";
        else if (ast->as<ASTDropAccessEntityQuery>())
            return "DROP_ACCESS_ENTITY";
        else if (ast->as<ASTGrantQuery>())
            return "GRANT";
        else if (ast->as<ASTShowCreateAccessEntityQuery>())
            return "SHOW_CREATE_ACCESS_ENTITY";
        else if (ast->as<ASTShowGrantsQuery>())
            return "SHOW_GRANT";
        else if (ast->as<ASTShowAccessEntitiesQuery>())
            return "SHOW_ACCESS_ENTITIES";
        else if (ast->as<ASTShowAccessQuery>())
            return "SHOW_ACCESS";
        else if (ast->as<ASTShowPrivilegesQuery>())
            return "SHOW_PRIV";
        else if (ast->as<ASTExternalDDLQuery>())
            return "EXTERNAL_DDL";
        else if (ast->as<Streaming::ASTRecoverQuery>())
            return "RECOVER";
        else if (ast->as<Streaming::ASTUnsubscribeQuery>())
            return "UNSUBSCRIBE";
        else
            return "UNKNOWN";
    }
}

std::pair<String, Int32> SQLAnalyzerRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & query = payload->get("query").toString();
    ParserQuery parser(query.c_str() + query.size());

    query_context->setCollectRequiredColumns(true);
    const auto & settings = query_context->getSettingsRef();

    String error_msg;
    /// auto res = rewriteQueryPipeAndParse(
    ///    parser, query.c_str(), query.c_str() + query.size(), error_msg, false, settings.max_query_size, settings.max_parser_depth);

    auto * begin = query.c_str();
    auto * end = query.c_str() + query.size();
    auto ast = tryParseQuery(parser, begin, end, error_msg, false, "analyzer", false, settings.max_query_size, settings.max_parser_depth);

    if (error_msg.empty())
    {
        /// auto & [rewritten_query, ast] = res;

        /// LOG_DEBUG(log, "Query rewrite, query_id={} rewritten={}", query_context->getCurrentQueryId(), rewritten_query);

        QueryProfileMatcher::Data profile;
        QueryProfileVisitor visitor(profile);
        visitor.visit(ast);

        /// Propagate WITH statement to children ASTSelect.
        if (settings.enable_global_with_statement)
        {
            ApplyWithGlobalVisitor().visit(ast);
        }

        Block block;

        /// FIXME: CREATE STREAM ... AS SELECT ...
        /// FIXME: INSERT INTO STREAM ... SELECT ...
        bool has_aggr = false;
        bool is_streaming = true;
        std::set<String> group_by_columns;

        if (auto * const select = ast->as<ASTSelectWithUnionQuery>())
        {
            /// Interpreter will trigger ast analysis. One side effect is collecting
            /// required columns during the analysis process
            InterpreterSelectWithUnionQuery interpreter(ast, query_context, SelectQueryOptions());
            has_aggr = interpreter.hasAggregation();
            block = interpreter.getSampleBlock();
            is_streaming = interpreter.isStreamingQuery();
            group_by_columns = interpreter.getGroupByColumns();
        }

        auto query_type = queryType(ast);
        return {
            buildResponse(
                query,
                /// rewritten_query,
                query,
                query_type,
                profile,
                block,
                has_aggr,
                group_by_columns,
                is_streaming,
                query_context->requiredColumns()),
            HTTPResponse::HTTP_OK};
    }
    else
    {
        LOG_ERROR(log, "Query analyzer, query_id={} error_msg={}", query_context->getCurrentQueryId(), error_msg);

        return {jsonErrorResponse(error_msg, ErrorCodes::INCORRECT_QUERY), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

bool SQLAnalyzerRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(POST_SCHEMA, payload, error_msg))
    {
        return false;
    }

    const auto & query = payload->get("query").toString();
    if (query.empty())
    {
        error_msg = "Empty query";
        return false;
    }
    return true;
}
}
