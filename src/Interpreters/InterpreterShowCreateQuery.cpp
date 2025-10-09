#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/typeid_cast.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
    extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock() const
{
    /// proton : starts
    const auto & settings = getContext()->getSettingsRef();
    if (settings.show_multi_versions)
        return Block{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "statement"},
            {ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "last_modified"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"},
            {ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "uuid"},
        };
    else if (settings.verbose && (query_ptr->as<ASTShowCreateTableQuery>() != nullptr))
        return Block{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "statement"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "placements"},
        };
    else
        return Block{{
            ColumnString::create(),
            std::make_shared<DataTypeString>(),
            "statement"}};
    /// proton : ends
}


QueryPipeline InterpreterShowCreateQuery::executeImpl()
{
    auto query_context = getContext();
    auto table_id = StorageID::createEmpty();

    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateViewQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        auto resolve_table_type = show_query->temporary ? Context::ResolveExternal : Context::ResolveOrdinary;
        table_id = query_context->resolveStorageID(*show_query, resolve_table_type);

        bool is_dictionary = static_cast<bool>(query_ptr->as<ASTShowCreateDictionaryQuery>());

        if (is_dictionary)
            query_context->checkAccess(AccessType::SHOW_DICTIONARIES, table_id);
        else
            query_context->checkAccess(AccessType::SHOW_COLUMNS, table_id);

        create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, query_context);

        auto & ast_create_query = create_query->as<ASTCreateQuery &>();
        if (query_ptr->as<ASTShowCreateViewQuery>())
        {
            if (!ast_create_query.isView())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a VIEW",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
        else if (is_dictionary)
        {
            if (!ast_create_query.isDictionary())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a DICTIONARY",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->temporary)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary databases are not possible.");
        show_query->setDatabase(query_context->resolveDatabase(show_query->getDatabase()));
        query_context->checkAccess(AccessType::SHOW_DATABASES, show_query->getDatabase());
        create_query = DatabaseCatalog::instance().getDatabase(show_query->getDatabase())->getCreateDatabaseQuery();
    }

    if (!create_query)
        throw Exception(ErrorCodes::THERE_IS_NO_QUERY,
                        "Unable to show the create query of {}. Maybe it was created by the system.",
                        show_query->getTable());

    /// proton : starts
    if (query_context->getSettingsRef().show_multi_versions)
        return showMultiVersions(table_id);
    /// proton : ends

    if (!query_context->getSettingsRef().show_uuid)
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;
    }

    String res = create_query->formatWithPossiblyHidingSensitiveData(/*max_length=*/0, /*one_line=*/false, /*show_secrets=*/false);

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}}));
}

}
