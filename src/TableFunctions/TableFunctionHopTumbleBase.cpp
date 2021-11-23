#include "TableFunctionHopTumbleBase.h"

#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
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
#include <Common/ProtonCommon.h>

namespace DB
{
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
    return StreamingDistributedMergeTree::create(storage_id, columns, context, streaming_func_desc, timestamp_expr, timestamp_expr_required_columns);
}

void TableFunctionHopTumbleBase::init(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast)
{
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    if (storage->getName() != "DistributedMergeTree")
        throw Exception("Storage engine is not DistributedMergeTree", ErrorCodes::BAD_ARGUMENTS);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    columns = metadata_snapshot->getColumns();

    /// We will first need analyze time column expression since the streaming window function depends on the result of time column expr
    if (timestamp_expr_ast)
    {
        auto syntax_analyzer_result = TreeRewriter(context).analyze(timestamp_expr_ast, columns.getAll(), storage, metadata_snapshot);
        ExpressionAnalyzer expr_analyzer(timestamp_expr_ast, syntax_analyzer_result, context);

        timestamp_expr = expr_analyzer.getActions(true);
        timestamp_expr_required_columns = timestamp_expr->getRequiredColumns();

        /// Check the resulting type. It shall be a datetime / datetime64.
        const auto & time_column = timestamp_expr->getSampleBlock().getByPosition(0);
        assert(time_column.name == STREAMING_TIMESTAMP_ALIAS);
        if (!isDateTime(time_column.type) && isDateTime64(time_column.type))
            throw Exception("The resulting type of time column expression shall be DateTime or DateTime64", ErrorCodes::BAD_ARGUMENTS);

        ColumnDescription time_column_desc(STREAMING_TIMESTAMP_ALIAS, time_column.type);
        columns.add(time_column_desc);

        /// We need rewrite streaming function ast to depend on the time expression resulting column directly
        auto * node = streaming_func_ast->as<ASTFunction>();
        node->arguments->children[0] = std::make_shared<ASTIdentifier>(STREAMING_TIMESTAMP_ALIAS);
    }

    auto func_syntax_analyzer_result = TreeRewriter(context).analyze(streaming_func_ast, columns.getAll(), nullptr, nullptr);

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

            for (const auto * node : action.node->children)
            {
                argument_names.push_back(node->result_name);
                argument_types.push_back(node->result_type);
            }
            streaming_func_desc = std::make_shared<StreamingFunctionDescription>(
                streaming_func_ast,
                argument_names,
                argument_types,
                streaming_func_expr,
                func_syntax_analyzer_result->requiredSourceColumns());
        }
    }

    assert(streaming_func_desc);

    /// Parsing the result type of the streaming win function
    const auto & streaming_win_block = streaming_func_expr->getSampleBlock();
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
