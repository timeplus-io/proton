#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>

#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/QueryParameterVisitor.h>

/// proton: starts.
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Common/ProtonCommon.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}


namespace
{

bool isNullableOrLcNullable(DataTypePtr type)
{
    if (type->isNullable())
        return true;

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return lc_type->getDictionaryType()->isNullable();

    return false;
}

/// Returns `true` if there are nullable column in src but corresponding column in dst is not
bool changedNullabilityOneWay(const Block & src_block, const Block & dst_block)
{
    std::unordered_map<String, bool> src_nullable;
    for (const auto & col : src_block)
        src_nullable[col.name] = isNullableOrLcNullable(col.type);

    for (const auto & col : dst_block)
    {
        if (!isNullableOrLcNullable(col.type) && src_nullable[col.name])
            return true;
    }
    return false;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

bool hasJoin(const ASTSelectWithUnionQuery & ast)
{
    for (const auto & child : ast.list_of_selects->children)
    {
        if (const auto * select = child->as<ASTSelectQuery>(); select && hasJoin(*select))
            return true;
    }
    return false;
}

}

StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment,
    ContextPtr context_,
    const bool is_parameterized_view_)
    : IStorage(table_id_), local_context(Context::createCopy(context_))
{
    local_context->makeQueryContext();

    StorageInMemoryMetadata storage_metadata;
   if (!is_parameterized_view_)
    {
        /// If CREATE query is to create parameterized view, then we dont want to set columns
        if (!query.isParameterizedView())
            storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setComment(comment);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    /// proton: start.
    auto description = SelectQueryDescription::getSelectQueryFromASTForView(query.select->ptr(), local_context);
    if (std::ranges::find(description.select_table_ids, getStorageID()) != description.select_table_ids.end())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} {} cannot select from itself", getName(), getStorageID().getFullTableName());
    /// proton; ends.

    is_parameterized_view = is_parameterized_view_ || query.isParameterizedView();
    storage_metadata.setSelectQuery(description);
    setInMemoryMetadata(storage_metadata);
}

void StorageView::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const size_t /*num_streams*/)
{
    ASTPtr current_inner_query = storage_snapshot->metadata->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized VIEW query");
        current_inner_query = query_info.view_query->clone();
    }

    /// proton: starts.
    Streaming::rewriteSubquery(current_inner_query->as<ASTSelectWithUnionQuery &>(), query_info);
    /// proton: ends.

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 1, true, query_info.settings_limit_offset_done);
    InterpreterSelectWithUnionQuery interpreter(current_inner_query, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    interpreter.buildQueryPlan(query_plan);

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    auto materializing_actions = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    materializing_actions->addMaterializingOutputActions();

    auto materializing = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(materializing_actions));
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    /// And also convert to expected structure.
    const auto & expected_header = storage_snapshot->getSampleBlockForColumns(column_names);
    const auto & header = query_plan.getCurrentDataStream().header;

    const auto * select_with_union = current_inner_query->as<ASTSelectWithUnionQuery>();
    if (select_with_union && hasJoin(*select_with_union) && changedNullabilityOneWay(header, expected_header))
    {
        throw DB::Exception(ErrorCodes::INCORRECT_QUERY,
                            "Query from view {} returned Nullable column having not Nullable type in structure. "
                            "If query from view has JOIN, it may be cause by different values of 'join_use_nulls' setting. "
                            "You may explicitly specify 'join_use_nulls' in 'CREATE VIEW' query to avoid this error",
                            getStorageID().getFullTableName());
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
    converting->setStepDescription("Convert VIEW subquery result to VIEW stream structure");
    query_plan.addStep(std::move(converting));
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: no stream expression in view select AST");

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: incorrect stream expression");

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceQueryParametersIfParametrizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values)
{
    ReplaceQueryParameterVisitor visitor(parameter_values);
    visitor.visit(outer_query);
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        /// If it's a view or merge table function, add a fake db.table name.
        /// For parameterized view, the function name is the db.view name, so add the function name
        if (table_expression->table_function)
        {
            auto table_function_name = table_expression->table_function->as<ASTFunction>()->name;
            if (table_function_name == "view" || table_function_name == "viewIfPermitted")
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>("__view");
            /// proton: starts. (TODO: remove this after merge https://github.com/ClickHouse/ClickHouse/pull/40734)
            /// else if (table_function_name == "merge")
            ///    table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>("__merge");
            /// proton: ends.
            else if (parameterized_view)
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>(table_function_name);

        }

        if (!table_expression->database_and_table_name)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: incorrect stream expression");
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
        else if (child.get()
                 && child->as<ASTFunction>()
                 && table_expression->table_function
                 && table_expression->table_function->as<ASTFunction>()
                 && child->as<ASTFunction>()->name == table_expression->table_function->as<ASTFunction>()->name)
            child = view_query;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: incorrect stream expression");

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

/// proton: starts.
void StorageView::startup()
{
    if (started.test_and_set())
        return;

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_id = getStorageID();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    /// proton: starts. only add dependency for non-parameterized views.
    if (!is_parameterized_view)
    {
        for (const auto & select_table_id : select_query.select_table_ids)
            DatabaseCatalog::instance().addDependency(select_table_id, storage_id);
    }
    /// proton: ends.
}

void StorageView::shutdown(bool /*dropping*/)
{
    if (started.test())
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        auto storage_id = getStorageID();
        const auto & select_query = metadata_snapshot->getSelectQuery();
        /// proton: starts. only remove dependency for non-parameterized views.
        if (!is_parameterized_view)
        {
            for (const auto & select_table_id : select_query.select_table_ids)
                DatabaseCatalog::instance().removeDependency(select_table_id, storage_id);
        }
        /// proton: ends.
    }
}

bool StorageView::isReady() const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
    {
        auto source_storage = DatabaseCatalog::instance().getTable(select_table_id, local_context);
        if (!source_storage->isReady())
            return false;
    }
    return true;
}

bool StorageView::isStreamingQuery(ContextPtr query_context) const
{
    auto select = getInMemoryMetadataPtr()->getSelectQuery().inner_query;
    auto local_ctx = Context::createCopy(query_context);
    local_ctx->setCollectRequiredColumns(false);

    if (is_parameterized_view)
        return InterpreterSelectWithUnionQuery(select, local_ctx, SelectQueryOptions().createParameterizedView().noModify().analyze()).isStreamingQuery();
    else
        return InterpreterSelectWithUnionQuery(select, local_ctx, SelectQueryOptions().noModify().analyze()).isStreamingQuery();
}

bool StorageView::hasStreamingGlobalAggregation() const
{
    auto select = getInMemoryMetadataPtr()->getSelectQuery().inner_query;
    auto ctx = Context::createCopy(local_context);
    ctx->setCollectRequiredColumns(false);

    if (is_parameterized_view)
        return InterpreterSelectWithUnionQuery(select, ctx, SelectQueryOptions().createParameterizedView().noModify().analyze()).hasStreamingGlobalAggregation();
    else
        return InterpreterSelectWithUnionQuery(select, ctx, SelectQueryOptions().noModify().analyze()).hasStreamingGlobalAggregation();
}

Streaming::DataStreamSemanticEx StorageView::dataStreamSemantic() const
{
    if (data_stream_semantic_resolved)
        return data_stream_semantic;

    auto select = getInMemoryMetadataPtr()->getSelectQuery().inner_query;
    auto ctx = Context::createCopy(local_context);
    ctx->setCollectRequiredColumns(false);

    if (is_parameterized_view)
        data_stream_semantic = InterpreterSelectWithUnionQuery(select, ctx, SelectQueryOptions().createParameterizedView().noModify().analyze()).getDataStreamSemantic();
    else
        data_stream_semantic = InterpreterSelectWithUnionQuery(select, ctx, SelectQueryOptions().noModify().analyze()).getDataStreamSemantic();

    data_stream_semantic_resolved = true;

    return data_stream_semantic;
}

NamesAndTypesList StorageView::getVirtuals() const
{
    /// We may emit _tp_delta on the fly
    return {NameAndTypePair(ProtonConsts::RESERVED_DELTA_FLAG, DataTypeFactory::instance().get("int8"))};
}
/// proton: ends.

void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a View");

        return StorageView::create(args.table_id, args.query, args.columns, args.comment, args.getLocalContext(), /*is_parameterized_view*/ false);
    });
}

}
