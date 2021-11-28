#include "TableFunctionHopTumbleBase.h"

#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Storages/DistributedMergeTree/StreamingDistributedMergeTree.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace
{
    StreamingFunctionDescriptionPtr createStreamingFunctionDescription(
        ASTPtr ast, TreeRewriterResultPtr syntax_analyzer_result, ContextPtr context, const String & func_name_prefix)
    {
        ExpressionAnalyzer func_expr_analyzer(ast, syntax_analyzer_result, context);

        auto streaming_func_expr = func_expr_analyzer.getActions(true);
        /// Loop actions to figure out input argument types
        const auto & actions = streaming_func_expr->getActions();
        for (const auto & action : actions)
        {
            if (action.node->type == ActionsDAG::ActionType::FUNCTION && action.node->result_name.starts_with(func_name_prefix))
            {
                Names argument_names;
                argument_names.reserve(action.node->children.size());

                DataTypes argument_types;
                argument_types.reserve(action.node->children.size());

                for (const auto * node : action.node->children)
                {
                    argument_names.push_back(node->result_name);
                    argument_types.push_back(node->result_type);
                }
                return std::make_shared<StreamingFunctionDescription>(
                    ast, argument_names, argument_types, streaming_func_expr, syntax_analyzer_result->requiredSourceColumns());
            }
        }

        /// The timestamp function ends up with const column, like toDateTime('2020-01-01 00:00:00') or now('UTC') or now64(3, 'UTC')
        /// Check the function name is now or now64 since these are the only const function we support
        auto & func_name = ast->as<ASTFunction>()->name;
        boost::to_lower(func_name);

        if (func_name != "now" && func_name != "now64")
            throw Exception("Unsupported const timestamp func", ErrorCodes::BAD_ARGUMENTS);

        /// Parse the argument names
        return std::make_shared<StreamingFunctionDescription>(
            ast, Names{}, DataTypes{}, streaming_func_expr, syntax_analyzer_result->requiredSourceColumns());
    }
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StorageID TableFunctionHopTumbleBase::resolveStorageID(const ASTPtr & arg, ContextPtr context)
{
    String table;
    if (!tryGetIdentifierNameInto(arg, table))
        throw Exception("First argument must be table name", ErrorCodes::BAD_ARGUMENTS);

    ParserCompoundIdentifier table_name_p(true);

    Tokens tokens(table.data(), table.data() + table.size(), context->getSettingsRef().max_query_size);
    IParser::Pos pos(tokens, context->getSettingsRef().max_parser_depth);
    Expected expected;

    ASTPtr table_ast;
    if (!table_name_p.parse(pos, table_ast, expected))
        throw Exception("First argument is an invalid table name", ErrorCodes::BAD_ARGUMENTS);

    auto storage_id = table_ast->as<ASTTableIdentifier>()->getTableId();
    if (storage_id.database_name.empty())
        storage_id.database_name = context->getCurrentDatabase();

    /// return the storage ID with UUID
    return DatabaseCatalog::instance().getTable(storage_id, context)->getStorageID();
}

TableFunctionHopTumbleBase::TableFunctionHopTumbleBase(const String & name_) : name(name_)
{
}

StoragePtr TableFunctionHopTumbleBase::executeImpl(
    const ASTPtr & /* func_ast */, ContextPtr context, const String & /* table_name */, ColumnsDescription /* cached_columns_ = {} */) const
{
    return StreamingDistributedMergeTree::create(
        storage_id, columns, underlying_storage_metadata_snapshot, context, streaming_func_desc, timestamp_func_desc);
}

void TableFunctionHopTumbleBase::init(
    ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast)
{
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    if (storage->getName() != "DistributedMergeTree")
        throw Exception("Storage engine is not DistributedMergeTree", ErrorCodes::BAD_ARGUMENTS);

    underlying_storage_metadata_snapshot = storage->getInMemoryMetadataPtr();
    columns = underlying_storage_metadata_snapshot->getColumns();

    /// We will first need analyze time column expression since the streaming window function depends on the result of time column expr
    if (timestamp_expr_ast)
    {
        auto syntax_analyzer_result
            = TreeRewriter(context).analyze(timestamp_expr_ast, columns.getAll(), storage, underlying_storage_metadata_snapshot);
        timestamp_func_desc = createStreamingFunctionDescription(timestamp_expr_ast, std::move(syntax_analyzer_result), context, "");

        /// Check the resulting type. It shall be a datetime / datetime64.
        const auto & time_column = timestamp_func_desc->expr->getSampleBlock().getByPosition(0);
        assert(time_column.name == STREAMING_TIMESTAMP_ALIAS);
        if (!isDateTime(time_column.type) && !isDateTime64(time_column.type))
            throw Exception("The resulting type of time column expression shall be DateTime or DateTime64", ErrorCodes::BAD_ARGUMENTS);

        auto * node = streaming_func_ast->as<ASTFunction>();
        /// We need rewrite streaming function ast to depend on the time expression resulting column directly
        /// The following ast / expression analysis for streaming func will pick up this rewritten timestamp expr ast
        node->arguments->children[0] = std::make_shared<ASTIdentifier>(STREAMING_TIMESTAMP_ALIAS);

        ColumnDescription time_column_desc(STREAMING_TIMESTAMP_ALIAS, time_column.type);
        columns.add(time_column_desc);
    }

    auto func_syntax_analyzer_result = TreeRewriter(context).analyze(streaming_func_ast, columns.getAll(), nullptr, nullptr);
    streaming_func_desc
        = createStreamingFunctionDescription(streaming_func_ast, std::move(func_syntax_analyzer_result), context, func_name_prefix);

    /// Parsing the result type of the streaming win function
    const auto & streaming_win_block = streaming_func_desc->expr->getSampleBlock();
    assert(streaming_win_block.columns() >= 1);

    const auto & result_type_and_name = streaming_win_block.getByPosition(streaming_win_block.columns() - 1);
    handleResultType(result_type_and_name);
}

void TableFunctionHopTumbleBase::handleResultType(const ColumnWithTypeAndName & type_and_name)
{
    auto tuple_result_type = checkAndGetDataType<DataTypeTuple>(type_and_name.type.get());
    assert(tuple_result_type);
    assert(tuple_result_type->getElements().size() == 2);

    /// If streaming table function is used, we will need project `wstart, wend` columns to metadata
    DataTypePtr element_type = getElementType(tuple_result_type);

    ColumnDescription wstart(STREAMING_WINDOW_START, element_type);
    columns.add(wstart);

    ColumnDescription wend(STREAMING_WINDOW_END, element_type);
    columns.add(wend);
}

ColumnsDescription TableFunctionHopTumbleBase::getActualTableStructure(ContextPtr /* context */) const
{
    return columns;
}
}
