#include <Interpreters/InterpreterShowCreateAlertQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/GetAlertRequest.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateAlertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowCreateAlertQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <ranges>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterShowCreateAlertQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

Block InterpreterShowCreateAlertQuery::getSampleBlock() const
{
    if (getContext()->getSettingsRef().show_multi_versions)
        return Block{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "statement"},
            {ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "last_modified"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"},
        };
    else
        return Block{{ColumnString::create(), std::make_shared<DataTypeString>(), "statement"}};
}

QueryPipeline InterpreterShowCreateAlertQuery::executeImpl()
{
    const auto * show_query = query_ptr->as<ASTShowCreateAlertQuery>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_ALERTS);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    std::string database;
    if (show_query->database)
    {
        database = show_query->getDatabase();
        if (!DatabaseCatalog::instance().isDatabaseExist(database))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database);
    }
    else
    {
        database = current_context->getCurrentDatabase();
    }

    const auto & meta_store = Globals::getMetaStore();
    auto show_multi_versions = current_context->getSettingsRef().show_multi_versions.value;

    auto req = std::make_shared<cluster::GetAlertRequest>(
        database,
        show_query->getName(),
        /*versions_requested=*/show_multi_versions ? meta_store.getConfig().metadata_keep_versions : 1,
        meta_store.nodeID(),
        /*consistent_read=*/false,
        /*timeout_ms=*/5000,
        /*request_version=*/1);

    auto & metastore = Globals::getMetaStore();
    auto resp = metastore.getAlert(std::move(req));
    if (resp->hasError())
        throw Exception(
            resp->error().error_code,
            "Failed to load alert {} in database {}, error={}",
            show_query->getName(),
            database,
            resp->error().string());

    ASTCreateAlertQuery create_query;
    create_query.database = std::make_shared<ASTIdentifier>(database);
    create_query.children.push_back(create_query.database);
    create_query.name = show_query->name;
    create_query.children.push_back(create_query.name);
    create_query.children.push_back(nullptr); /// for create_query.select

    const auto & resp_data = resp->data();
    Block result = getSampleBlock();
    result.reserve(resp_data.descs.size());
    auto columns = result.mutateColumns();

    ParserSelectQuery p;
    for (const auto & desc : std::ranges::reverse_view(resp_data.descs))
    {
        Tokens tokens{desc->select_query.data(), desc->select_query.data() + desc->select_query.size()};
        IParser::Pos pos{tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH};
        Expected expected;
        ASTPtr select_ast;
        p.parse(pos, select_ast, expected);

        create_query.children.erase(create_query.children.begin() + create_query.children.size() - 1);
        create_query.set(create_query.select, select_ast);

        create_query.function_name = desc->function_name;

        create_query.batch_size = desc->batch.count;
        create_query.batch_timeout = desc->batch.interval;
        create_query.batch_timeout_unit = desc->batch.interval_kind;

        create_query.limit_count = desc->limit.count;
        create_query.limit_interval = desc->limit.interval;
        create_query.limit_interval_kind = desc->limit.interval_kind;

        WriteBufferFromOwnString buf;
        formatAST(create_query, buf, false, false);
        String statement = buf.str();

        columns[0]->insertData(statement.c_str(), statement.size());
        if (show_multi_versions)
        {
            /// 1 - version
            /// 2 - last_modified
            /// 3 - last_modified_by
            /// 4 - created
            /// 5 - created_by
            columns[1]->insert(desc->data_version);
            columns[2]->insert(DateTime64(desc->last_modify_timestamp_ms));
            columns[3]->insertData(desc->last_modified_by.data(), desc->last_modified_by.size());
            columns[4]->insert(DateTime64(desc->create_timestamp_ms));
            columns[5]->insertData(desc->created_by.data(), desc->created_by.size());
        }
    }

    result.setColumns(std::move(columns));
    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result)));
}

}
