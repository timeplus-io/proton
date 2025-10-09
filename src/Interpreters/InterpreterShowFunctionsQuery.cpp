#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Interpreters/InterpreterShowFunctionsQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Common/logger_useful.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <Cluster/Requests/ListUserDefinedFunctionsResponse.h>

namespace DB
{

BlockIO InterpreterShowFunctionsQuery::execute()
{
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FUNCTIONS);
    getContext()->checkAccess(access_rights_elements);

    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

QueryPipeline InterpreterShowFunctionsQuery::executeImpl()
{
    auto current_context = getContext();
    /// Get AST
    auto & query = query_ptr->as<ASTShowFunctionsQuery &>();
    auto where_expression = query.where_expression;

    /// Get udf description
    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    /// proton: starts - Single-instance: create request with defaults
    auto list_udf_req = std::make_shared<cluster::ListUserDefinedFunctionsRequest>("" /* function_name */, 0 /* node_id */, /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    auto list_udf_resp = metastore.listUserDefinedFunctions(list_udf_req);
    /// proton: ends
    if (list_udf_resp->hasError())
    {
        auto logger = getLogger("UserDefinedFunctionFactory");
        if (list_udf_resp->hasError())
        {
            LOG_ERROR(logger, "listAllUserDefinedFunctions failed");
            return {};
        }
    }
    auto & udf_descs = list_udf_resp->data().descs;

    /// Build the result column.
    MutableColumnPtr name_col = ColumnString::create();
    MutableColumnPtr type_col = ColumnString::create();
    MutableColumnPtr language_col = ColumnString::create();

    for (const auto & udf_desc : udf_descs)
    {
        name_col->insert(udf_desc->name);
        type_col->insert(udf_desc->payload->isAggregation() ? "Aggregate Function" : "Function");
        language_col->insert(magic_enum::enum_name(udf_desc->type()));
    }

    auto block = Block{
        {std::move(name_col), std::make_shared<DataTypeString>(), "name"},
        {std::move(type_col), std::make_shared<DataTypeString>(), "type"},
        {std::move(language_col), std::make_shared<DataTypeString>(), "language"}};
    /// SHOW FUNCTIONS where xxx
    if (where_expression)
    {
        auto syntax_analyzer_result = TreeRewriter(current_context).analyze(where_expression, block.getNamesAndTypesList());
        ExpressionAnalyzer where_expr_analyzer(where_expression, syntax_analyzer_result, current_context);

        auto dag = std::make_shared<ActionsDAG>(block.getNamesAndTypesList());
        dag = ActionsDAG::merge(std::move(*where_expr_analyzer.getActionsDAG(true, false)), std::move(*dag));
        auto actions = std::make_shared<ExpressionActions>(dag, ExpressionActionsSettings::fromContext(current_context));

        auto filter = std::make_shared<FilterTransform>(block.cloneEmpty(), actions, where_expression->getColumnName(), true);

        auto chunk = Chunk(block.getColumns(), block.rows());

        filter->transform(chunk);

        auto header = filter->getOutputs().front().getHeader();
        return QueryPipeline(std::make_shared<SourceFromSingleChunk>(header, std::move(chunk)));
    }
    /// SHOW FUNCTIONS
    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(block));
}
}
