#include "TableFunctionWindow.h"

#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Streaming/ASTSessionRangeComparision.h>
#include <Storages/StorageView.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Streaming
{
void TableFunctionWindow::doParseArguments(const ASTPtr & func_ast, ContextPtr context, const String & help_msg)
{
    /// Please note logic here is actually tightly tailed for tumble/hop table function.
    /// This is not neat, but it is ok now.
    if (func_ast->children.size() != 1)
        throw Exception(help_msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto streaming_func_ast = func_ast->clone();
    auto * node = streaming_func_ast->as<ASTFunction>();
    assert(node);

    auto args{checkAndExtractArguments(node)};

    /// First argument is expected to be table
    resolveStorageID(args[0], context);

    /// The rest of the arguments are streaming window arguments
    /// Change the name to call the internal streaming window functions
    node->name = "__" + node->name;

    /// Prune the arguments to fit the internal window function
    args.erase(args.begin());

    ASTPtr timestamp_expr_ast;

    //// [timestamp_column_expr]
    /// The following logic is adding system default time column to tumble function if user doesn't specify one
    if (args[0])
    {
        if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, tumble(table, toDateTime32(t), INTERVAL 5 SECOND)
            func_node->alias = ProtonConsts::STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }
    else
    {
        /// We like to validate if the RESERVED_EVENT_TIME is an alias column
        if (!subquery)
        {
            auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
            assert(storage);
            auto metadata{storage->getInMemoryMetadataPtr()};
            if (metadata->columns.has(ProtonConsts::RESERVED_EVENT_TIME))
            {
                const auto & col_desc = metadata->columns.get(ProtonConsts::RESERVED_EVENT_TIME);
                if (col_desc.default_desc.kind == ColumnDefaultKind::Alias)
                {
                    args[0] = col_desc.default_desc.expression;
                    args[0]->setAlias(ProtonConsts::STREAMING_TIMESTAMP_ALIAS);
                    timestamp_expr_ast = args[0];
                }
            }
        }

        if (!timestamp_expr_ast)
            args[0] = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME);
    }

    postArgs(args);

    node->arguments->children.swap(args);

    /// Calculate column description
    init(std::move(context), std::move(streaming_func_ast), functionNamePrefix(), std::move(timestamp_expr_ast));
}

void TableFunctionWindow::init(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast)
{
    auto storage = calculateColumnDescriptions(context);

    /// We will first need analyze time column expression since the streaming window function depends on the result of time column expr
    if (timestamp_expr_ast)
    {
        auto syntax_analyzer_result = TreeRewriter(context).analyze(
            timestamp_expr_ast, columns.getAll(), storage ? storage : nullptr, storage ? underlying_storage_snapshot : nullptr);
        timestamp_func_desc = createStreamingFunctionDescription(timestamp_expr_ast, std::move(syntax_analyzer_result), context, "");

        /// Check the resulting type. It shall be a datetime / datetime64.
        const auto & time_column = timestamp_func_desc->expr->getSampleBlock().getByPosition(0);
        assert(time_column.name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS);
        if (!isDateTime(time_column.type) && !isDateTime64(time_column.type))
            throw Exception("The resulting type of time column expression shall be datetime or datetime64", ErrorCodes::BAD_ARGUMENTS);

        auto * node = streaming_func_ast->as<ASTFunction>();
        /// We need rewrite streaming function ast to depend on the time expression resulting column directly
        /// The following ast / expression analysis for streaming func will pick up this rewritten timestamp expr ast
        node->arguments->children[0] = std::make_shared<ASTIdentifier>(ProtonConsts::STREAMING_TIMESTAMP_ALIAS);

        ColumnDescription time_column_desc(ProtonConsts::STREAMING_TIMESTAMP_ALIAS, time_column.type);
        columns.add(time_column_desc);
    }

    auto func_syntax_analyzer_result = TreeRewriter(context).analyze(
        streaming_func_ast, columns.getAll(), storage ? storage : nullptr, storage ? underlying_storage_snapshot : nullptr);
    streaming_func_desc
        = createStreamingFunctionDescription(streaming_func_ast, std::move(func_syntax_analyzer_result), context, func_name_prefix);

    /// Parsing the result type of the streaming win function
    const auto & streaming_win_block = streaming_func_desc->expr->getSampleBlock();
    assert(streaming_win_block.columns() >= 1);

    const auto & result_type_and_name = streaming_win_block.getByPosition(streaming_win_block.columns() - 1);
    handleResultType(result_type_and_name);
}

void TableFunctionWindow::handleResultType(const ColumnWithTypeAndName & type_and_name)
{
    const auto * tuple_result_type = checkAndGetDataType<DataTypeTuple>(type_and_name.type.get());
    assert(tuple_result_type);
    assert(tuple_result_type->haveExplicitNames());
    size_t elem_size = tuple_result_type->getElements().size();
    assert(elem_size >= 2);

    /// If streaming table function is used, we will need project `wstart, wend ...` columns to metadata
    for (size_t i = 1; const auto & type : tuple_result_type->getElements())
        columns.add(ColumnDescription(tuple_result_type->getNameByPosition(i++), type));
}
}
}
