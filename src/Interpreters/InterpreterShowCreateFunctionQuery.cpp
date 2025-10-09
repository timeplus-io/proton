#include <Interpreters/InterpreterShowCreateFunctionQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowCreateFunctionQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FUNCTION;
}

BlockIO InterpreterShowCreateFunctionQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

Block InterpreterShowCreateFunctionQuery::getSampleBlock() const
{
    if (getContext()->getSettingsRef().show_multi_versions)
        /// FIXME, add last_modified, last_modified_by etc when we have that information
        return Block{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "statement"},
            {ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"},
        };
    else
        return Block{{ColumnString::create(), std::make_shared<DataTypeString>(), "statement"}};
}

QueryPipeline InterpreterShowCreateFunctionQuery::executeImpl()
{
    auto * show_query = query_ptr->as<ASTShowCreateFunctionQuery>();
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FUNCTIONS);
    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    const auto function_name = show_query->getFunctionName();
    if (current_context->getSettingsRef().show_multi_versions)
        return showMultiVersions(function_name);

    auto & metastore = Globals::getMetaStore();

    auto req = std::make_shared<cluster::GetUserDefinedFunctionRequest>(
        function_name,
        /*versions_requested_=*/1,
        current_context->getNodeID(),
        /*consistent_read_=*/false,
        /*timeout_ms_=*/5'000,
        /*request_version=*/1);
    auto response = metastore.getUserDefinedFunction(std::move(req));
    if (response->hasError())
        throw Exception(
            response->error().error_code, "Failed to get UserDefined function '{}', {}", function_name, response->error().string());

    MutableColumnPtr column = ColumnString::create();
    column->insert(response->data().descs.front()->createASTString());
    return QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}}));
}

QueryPipeline InterpreterShowCreateFunctionQuery::showMultiVersions(const String & name) const
{
    const auto & meta_store = Globals::getMetaStore();
    auto & metastore = Globals::getMetaStore();

    auto req = std::make_shared<cluster::GetUserDefinedFunctionRequest>(
        name,
        meta_store.getConfig().metadata_keep_versions,
        meta_store.nodeID(),
        /*consistent_read_=*/false,
        /*timeout_ms_=*/5'000,
        /*request_version=*/1);
    auto response = metastore.getUserDefinedFunction(std::move(req));
    if (response->hasError())
        throw Exception(
            response->error().error_code, "Failed to read all versions UserDefined function '{}', {}", name, response->error().string());

    const auto & resp_data = response->data();
    Block result = getSampleBlock();
    result.reserve(resp_data.descs.size());
    auto columns = result.mutateColumns();

    /// 0 - statement
    /// 1 - version
    for (auto riter = resp_data.descs.rbegin(); riter != resp_data.descs.rend(); ++riter)
    {
        const auto & desc = *riter;
        columns[0]->insert(desc->createASTString());
        columns[1]->insert(desc->data_version);
    }

    result.setColumns(std::move(columns));

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result)));
}

}
