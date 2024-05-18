#include <Interpreters/getTableExpressions.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>

/// proton: starts.
#include <Common/ProtonCommon.h>
#include <Storages/Streaming/storageUtil.h>
/// proton: ends.

namespace DB
{

NameSet removeDuplicateColumns(NamesAndTypesList & columns)
{
    NameSet names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            it = columns.erase(it);
    }
    return names;
}

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query)
{
    if (!select_query.tables())
        return {};

    std::vector<const ASTTableExpression *> tables_expression;

    for (const auto & child : select_query.tables()->children)
    {
        const auto * tables_element = child->as<ASTTablesInSelectQueryElement>();

        if (tables_element && tables_element->table_expression)
            tables_expression.emplace_back(tables_element->table_expression->as<ASTTableExpression>());
    }

    return tables_expression;
}

const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (!select.tables())
        return {};

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.size() <= table_number)
        return {};

    const auto & tables_element = tables_in_select_query.children[table_number]->as<ASTTablesInSelectQueryElement &>();

    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (const ASTTableExpression * table_expression = getTableExpression(select, table_number))
    {
        if (table_expression->database_and_table_name)
            return table_expression->database_and_table_name;

        if (table_expression->table_function)
            return table_expression->table_function;

        if (table_expression->subquery)
            return table_expression->subquery->children[0];
    }

    return nullptr;
}

static NamesAndTypesList getColumnsFromTableExpression(
    const ASTTableExpression & table_expression,
    ContextPtr context,
    NamesAndTypesList & materialized,
    NamesAndTypesList & aliases,
    NamesAndTypesList & virtuals,
    Streaming::DataStreamSemanticEx * output_data_stream_semantic)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context, true, output_data_stream_semantic).getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        auto query_context = context->getQueryContext();
        const auto & function_storage = query_context->executeTableFunction(table_expression.table_function);
        auto function_metadata_snapshot = function_storage->getInMemoryMetadataPtr();
        const auto & columns = function_metadata_snapshot->getColumns();
        names_and_type_list = columns.getOrdinary();
        materialized = columns.getMaterialized();
        aliases = columns.getAliases();
        virtuals = function_storage->getVirtuals();

        /// proton : starts. Calculate hash semantic
        if (output_data_stream_semantic)
        {
            *output_data_stream_semantic = function_storage->dataStreamSemantic();
            output_data_stream_semantic->streaming = isStreamingStorage(function_storage, context);
        }
        /// proton : ends
    }
    else if (table_expression.database_and_table_name)
    {
        auto table_id = context->resolveStorageID(table_expression.database_and_table_name);
        const auto & table = DatabaseCatalog::instance().getTable(table_id, context);
        auto table_metadata_snapshot = table->getInMemoryMetadataPtr();
        const auto & columns = table_metadata_snapshot->getColumns();
        names_and_type_list = columns.getOrdinary();
        materialized = columns.getMaterialized();
        aliases = columns.getAliases();
        virtuals = table->getVirtuals();

        /// proton : starts. Calculate \output_data_stream_semantic if exists
        if (output_data_stream_semantic)
        {
            *output_data_stream_semantic = table->dataStreamSemantic();
            output_data_stream_semantic->streaming = isStreamingStorage(table, context);
        }
        /// proton : ends
    }

    return names_and_type_list;
}

/// proton: starts.
static void removeReservedColumns(NamesAndTypesList & columns)
{
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (std::find(ProtonConsts::RESERVED_COLUMN_NAMES.begin(), ProtonConsts::RESERVED_COLUMN_NAMES.end(), it->name)
            != ProtonConsts::RESERVED_COLUMN_NAMES.end())
            it = columns.erase(it);
        else
            ++it;
    }
}
/// proton: ends.

TablesWithColumns getDatabaseAndTablesWithColumns(
    const ASTTableExprConstPtrs & table_expressions,
    ContextPtr context,
    bool include_alias_cols,
    bool include_materialized_cols)
{
    TablesWithColumns tables_with_columns;

    String current_database = context->getCurrentDatabase();

    for (const ASTTableExpression * table_expression : table_expressions)
    {
        NamesAndTypesList materialized;
        NamesAndTypesList aliases;
        NamesAndTypesList virtuals;
        /// proton: starts.
        Streaming::DataStreamSemanticEx output_date_stream_semantic;
        NamesAndTypesList names_and_types = getColumnsFromTableExpression(
            *table_expression, context, materialized, aliases, virtuals, &output_date_stream_semantic);
        /// proton: ends.

        removeDuplicateColumns(names_and_types);

        /// proton: starts.
        if (!context->getSettingsRef().asterisk_include_reserved_columns)
            removeReservedColumns(names_and_types);
        /// proton: ends.

        tables_with_columns.emplace_back(DatabaseAndTableWithAlias(*table_expression, current_database), names_and_types);

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(materialized);
        table.addHiddenColumns(aliases);
        table.addHiddenColumns(virtuals);

        if (include_alias_cols)
            table.addAliasColumns(aliases);

        if (include_materialized_cols)
            table.addMaterializedColumns(materialized);

        /// proton : starts
        table.addHiddenColumns(ColumnsDescription::getSubcolumns(table.columns));
        table.setOutputDataStreamSemantic(output_date_stream_semantic);
        /// proton : ends
    }

    return tables_with_columns;
}
}
