#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserDescribeCacheQuery.h>
#include <Parsers/QueryWithOutputSettingsPushDownVisitor.h>
#include <Parsers/Access/ParserShowAccessEntitiesQuery.h>
#include <Parsers/Access/ParserShowAccessQuery.h>
#include <Parsers/Access/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ParserShowGrantsQuery.h>
#include <Parsers/Access/ParserShowPrivilegesQuery.h>
#include <Common/Exception.h>

/// proton : starts
#include <Parsers/ParserDropExternalTableQuery.h>
#include <Parsers/ParserShowCreateFormatSchemaQuery.h>
#include <Parsers/ParserShowFormatSchemasQuery.h>
#include <Parsers/Streaming/ParserSubscribeQuery.h>
/// proton : ends


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserShowTablesQuery show_tables_p;
    ParserSelectWithUnionQuery select_p;
    Streaming::ParserSubscribeQuery subscribe_p;
    Streaming::ParserUnsubscribeQuery unsubscribe_p;
    Streaming::ParserRecoverQuery recover_p;
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p;
    ParserDescribeCacheQuery describe_cache_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p;
    ParserAlterQuery alter_p;
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserDropExternalTableQuery drop_external_table_p;
    ParserCheckQuery check_p;
    ParserOptimizeQuery optimize_p;
    ParserKillQueryQuery kill_query_p;
    ParserWatchQuery watch_p;
    ParserShowAccessQuery show_access_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;
    ParserExplainQuery explain_p(end, allow_settings_after_format_in_insert);
    /// proton: starts
    ParserShowFormatSchemasQuery show_format_schemas_p;
    ParserShowCreateFormatSchemaQuery show_create_format_schema_p;
    /// proton: ends

    ASTPtr query;

    bool parsed =
           explain_p.parse(pos, query, expected)
        || select_p.parse(pos, query, expected)
        || subscribe_p.parse(pos, query, expected)
        || unsubscribe_p.parse(pos, query, expected)
        || recover_p.parse(pos, query, expected)
        || show_create_access_entity_p.parse(pos, query, expected) /// should be before `show_tables_p`
        /// proton: starts
        || show_create_format_schema_p.parse(pos, query, expected) /// should be before 'show_tables_p'
        /// proton: ends
        || show_tables_p.parse(pos, query, expected)
        || table_p.parse(pos, query, expected)
        || describe_cache_p.parse(pos, query, expected)
        || describe_table_p.parse(pos, query, expected)
        || show_processlist_p.parse(pos, query, expected)
        || create_p.parse(pos, query, expected)
        || alter_p.parse(pos, query, expected)
        || rename_p.parse(pos, query, expected)
        || drop_p.parse(pos, query, expected)
        /// proton: starts
        || drop_external_table_p.parse(pos, query, expected)
        /// proton: ends
        || check_p.parse(pos, query, expected)
        || kill_query_p.parse(pos, query, expected)
        || optimize_p.parse(pos, query, expected)
        || watch_p.parse(pos, query, expected)
        /// proton: starts
        || show_format_schemas_p.parse(pos, query, expected)
        /// proton: ends
        || show_access_p.parse(pos, query, expected)
        || show_access_entities_p.parse(pos, query, expected)
        || show_grants_p.parse(pos, query, expected)
        || show_privileges_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    /// FIXME: try to prettify this cast using `as<>()`
    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile("INTO OUTFILE");
    if (s_into_outfile.ignore(pos, expected, false))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected))
            return false;

        ParserKeyword s_compression_method("COMPRESSION");
        if (s_compression_method.ignore(pos, expected))
        {
            ParserStringLiteral compression;
            if (!compression.parse(pos, query_with_output.compression, expected))
                return false;
        }

        query_with_output.children.push_back(query_with_output.out_file);
    }

    ParserKeyword s_format("FORMAT");

    if (s_format.ignore(pos, expected))
    {
        ParserIdentifier format_p;

        if (!format_p.parse(pos, query_with_output.format, expected))
            return false;
        setIdentifierSpecial(query_with_output.format);

        query_with_output.children.push_back(query_with_output.format);
    }

    // SETTINGS key1 = value1, key2 = value2, ...
    ParserKeyword s_settings("SETTINGS");
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, query_with_output.settings_ast, expected))
            return false;
        query_with_output.children.push_back(query_with_output.settings_ast);

        // SETTINGS after FORMAT is not parsed by the SELECT parser (ParserSelectQuery)
        // Pass them manually, to apply in InterpreterSelectQuery::initSettings()
        if (query->as<ASTSelectWithUnionQuery>())
        {
            auto settings = query_with_output.settings_ast->clone();
            assert_cast<ASTSetQuery *>(settings.get())->print_in_format = false;
            QueryWithOutputSettingsPushDownVisitor::Data data{settings};
            QueryWithOutputSettingsPushDownVisitor(data).visit(query);
        }
    }

    node = std::move(query);
    return true;
}

}
