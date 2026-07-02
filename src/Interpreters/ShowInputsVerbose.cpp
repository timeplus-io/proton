#include <Interpreters/ShowInputsVerbose.h>

#include <Access/Common/AccessFlags.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/likePatternToRegexp.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InputSettingsUtils.h>
#include <Interpreters/InputTargetStreams.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/FilterTransform.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/re2.h>

#include <algorithm>

namespace DB
{

namespace
{

Block getSampleBlock()
{
    return Block{
        {ColumnString::create(), std::make_shared<DataTypeString>(), "database"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "name"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "type"},
        {ColumnArray::create(ColumnString::create(), ColumnUInt64::create()),
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
         "target_streams"},
        {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"},
    };
}

bool nameMatchesLike(const ASTShowTablesQuery & query, const String & name)
{
    if (query.like.empty())
        return true;

    re2::RE2::Options options;
    options.set_case_sensitive(!query.case_insensitive_like);
    const re2::RE2 re(likePatternToRegexp(query.like), options);
    const bool matched = re.ok() && re2::RE2::FullMatch(name, re);
    return query.not_like ? !matched : matched;
}

std::optional<size_t> tryGetLimit(const ASTPtr & limit_expr, const ContextPtr & context)
{
    if (!limit_expr)
        return std::nullopt;

    auto literal_ast = evaluateConstantExpressionAsLiteral(limit_expr, context);
    const auto * literal = literal_ast ? literal_ast->as<ASTLiteral>() : nullptr;
    if (!literal)
        return std::nullopt;

    try
    {
        return static_cast<size_t>(literal->value.safeGet<UInt64>());
    }
    catch (...)
    {
        return std::nullopt;
    }
}

struct InputDescriptorInfo
{
    String type;
    std::vector<String> target_streams;
};

std::optional<InputDescriptorInfo> tryParseInputDescriptorInfo(const cluster::protocol::StreamDescriptor & desc, ContextPtr context)
{
    if (desc.sql_def.find("INPUT") == std::string::npos)
        return std::nullopt;

    ASTPtr ast;
    try
    {
        ast = DB::parseQuery(desc.sql_def, context);
    }
    catch (...)
    {
        return std::nullopt;
    }

    const auto * create = ast->as<ASTCreateQuery>();
    if (!create || !create->is_input)
        return std::nullopt;

    InputDescriptorInfo info;
    if (create->storage && create->storage->settings)
    {
        if (const Field * type_field = create->storage->settings->changes.tryGet("type");
            type_field && type_field->getType() == Field::Types::String)
        {
            info.type = type_field->safeGet<String>();
        }

        info.target_streams = extractTargetStreamValues(create->storage->settings);
    }

    if (info.target_streams.empty())
        info.target_streams = tryGetInputTargetStreamsFromRuntime(desc.stream.ns, desc.stream.name, context);

    return info;
}

}

QueryPipeline executeShowInputsVerbose(const ASTShowTablesQuery & query, ContextPtr context)
{
    context->checkAccess(AccessType::SHOW_TABLES);

    String database = context->resolveDatabase(query.from);
    DatabaseCatalog::instance().assertDatabaseExists(database);

    const auto & meta_store = Globals::getMetaStore();
    auto req = std::make_shared<cluster::ListStreamsRequest>(
        database,
        /*stream=*/"",
        /*initiator=*/meta_store.nodeID(),
        /*consistent_read=*/false,
        /*timeout_ms=*/5'000,
        /*request_version=*/2);

    /// Single-instance: read stream descriptors directly from the local meta-store
    /// instead of routing the request through the cluster app meta client.
    auto resp = meta_store.listStreams(std::move(req));
    if (resp->hasError())
        throw Exception(resp->error().error_code, "Failed to fetch inputs: {}", resp->error().error_message);

    auto block = getSampleBlock();
    const auto limit = tryGetLimit(query.limit_length, context);
    const bool has_where = static_cast<bool>(query.where_expression);

    auto columns = block.mutateColumns();
    for (const auto & desc_ptr : resp->data().stream_descs)
    {
        if (!desc_ptr)
            continue;
        const auto & desc = *desc_ptr;

        const auto & name = desc.stream.name;
        if (!nameMatchesLike(query, name))
            continue;
        auto maybe_info = tryParseInputDescriptorInfo(desc, context);
        if (!maybe_info)
            continue;

        size_t col_idx = 0;
        columns[col_idx++]->insertData(desc.stream.ns.data(), desc.stream.ns.size());
        columns[col_idx++]->insertData(name.data(), name.size());
        columns[col_idx++]->insert(maybe_info->type);

        Array targets;
        targets.reserve(maybe_info->target_streams.size());
        for (const auto & target : maybe_info->target_streams)
            targets.emplace_back(target);
        columns[col_idx++]->insert(targets);

        columns[col_idx++]->insert(DateTime64(desc.create_timestamp_ms));
        columns[col_idx++]->insertData(desc.last_modified_by.data(), desc.last_modified_by.size());
        columns[col_idx++]->insertData(desc.created_by.data(), desc.created_by.size());

        /// LIMIT is applied after WHERE to match SQL semantics. If there is no WHERE,
        /// we can apply it during iteration to avoid extra work.
        if (limit && !has_where && columns[0]->size() >= *limit)
            break;
    }
    block.setColumns(std::move(columns));

    if (query.where_expression)
    {
        auto where_expression = query.where_expression;
        auto syntax_analyzer_result = TreeRewriter(context).analyze(where_expression, block.getNamesAndTypesList());
        ExpressionAnalyzer where_expr_analyzer(where_expression, syntax_analyzer_result, context);

        auto dag = std::make_shared<ActionsDAG>(block.getNamesAndTypesList());
        dag = ActionsDAG::merge(std::move(*where_expr_analyzer.getActionsDAG(true, false)), std::move(*dag));
        auto actions = std::make_shared<ExpressionActions>(dag, ExpressionActionsSettings::fromContext(context));

        auto filter = std::make_shared<FilterTransform>(block.cloneEmpty(), actions, where_expression->getColumnName(), true);
        auto chunk = Chunk(block.getColumns(), block.rows());
        filter->transform(chunk);

        if (limit && chunk.getNumRows() > *limit)
        {
            Columns limited_columns;
            limited_columns.reserve(chunk.getNumColumns());
            for (const auto & col : chunk.getColumns())
                limited_columns.emplace_back(col->cut(0, *limit));
            chunk.setColumns(std::move(limited_columns), *limit);
        }

        auto header = filter->getOutputs().front().getHeader();
        return QueryPipeline(std::make_shared<SourceFromSingleChunk>(header, std::move(chunk)));
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
}

}
