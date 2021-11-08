#include "TableFunctionStreamingWindow.h"

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Storages/DistributedMergeTree/StreamingDistributedMergeTree.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StorageID TableFunctionStreamingWindow::resolveStorageID(const ASTPtr & arg, ContextPtr context)
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

TableFunctionStreamingWindow::TableFunctionStreamingWindow(const String & name_) : name(name_)
{
}

StoragePtr TableFunctionStreamingWindow::executeImpl(
    const ASTPtr & /* func_ast */, ContextPtr context, const String & /* table_name */, ColumnsDescription /* cached_columns_ = {} */) const
{
    return StreamingDistributedMergeTree::create(storage_id, columns, context, streaming_func_desc);
}

void TableFunctionStreamingWindow::initColumnsDescription(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix)
{
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    if (storage->getName() != "DistributedMergeTree")
        throw Exception("Storage engine is not DistributedMergeTree", ErrorCodes::BAD_ARGUMENTS);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    columns = metadata_snapshot->getColumns();

    auto func_syntax_analyzer_result = TreeRewriter(context).analyze(streaming_func_ast, columns.getAll(), storage, metadata_snapshot);

    ExpressionAnalyzer func_expr_analyzer(streaming_func_ast, func_syntax_analyzer_result, context);

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

            for (const auto node : action.node->children)
            {
                argument_names.push_back(node->result_name);
                argument_types.push_back(node->result_type);
            }
            streaming_func_desc = std::make_shared<StreamingFunctionDescription>(
                streaming_func_ast, argument_names, argument_types, streaming_func_expr, func_syntax_analyzer_result->requiredSourceColumns());
        }
    }

    assert(streaming_func_desc);

    /// Parsing the result type of the streaming win function
    const auto & streaming_win_block = streaming_func_expr->getSampleBlock();
    assert(streaming_win_block.columns() >= 1);

    const auto & result_type_and_name = streaming_win_block.getByPosition(streaming_win_block.columns() - 1);
    handleResultType(result_type_and_name);
}

ColumnsDescription TableFunctionStreamingWindow::getActualTableStructure(ContextPtr /* context */) const
{
    return columns;
}
}
