#include "TableFunctionDedup.h"

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

TableFunctionDedup::TableFunctionDedup(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires at least 2 parameters. The deduplication key column parameters shall not be constant. The last optional "
        "limit parameter shall be integer constant if present. For example, dedup(test, id, 1000). "
        "dedup(stream, column1[, column2, ..., [limit]])",
        name);
}

void TableFunctionDedup::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs asts;

    auto dedup_func_ast = func_ast->clone();
    auto * node = dedup_func_ast->as<ASTFunction>();
    assert(node);

    auto args{checkAndExtractArguments(node)};

    /// First argument is expected to be table
    resolveStorageID(args[0], context);

    /// Change the name to call the internal __dedup functions
    node->name = "__" + node->name;

    /// Prune the arguments to fit the internal window function
    args.erase(args.begin());

    node->arguments->children.swap(args);

    /// Calculate column description
    auto storage = TableFunctionProxyBase::calculateColumnDescriptions(context);

    auto syntax_analyzer_result = TreeRewriter(context).analyze(
        dedup_func_ast, columns.getAll(), storage ? storage : nullptr, storage ? underlying_storage_snapshot : nullptr);

    ExpressionAnalyzer func_expr_analyzer(dedup_func_ast, syntax_analyzer_result, context);

    streaming_func_desc = std::make_shared<StreamingFunctionDescription>(
        dedup_func_ast,
        WindowType::NONE,
        Names{},
        DataTypes{},
        func_expr_analyzer.getActions(true),
        syntax_analyzer_result->requiredSourceColumns(),
        ColumnNumbers{},
        false);
}

ASTs TableFunctionDedup::checkAndExtractArguments(ASTFunction * node) const
{
    /// dedup(table, column1, column2, ..., limit)
    const auto & args = node->arguments->children;
    if (args.size() < 2)
        throw Exception(help_message, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    size_t end_pos = args.size();
    if (auto * lit = args.back()->as<ASTLiteral>(); lit)
    {
        if (args.size() == 2)
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

        if (!isInt64OrUInt64FieldType(lit->value.getType()))
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

        end_pos -= 1;
    }

    for (size_t i = 1; i < end_pos; ++i)
        if (!args[i]->as<ASTFunction>() && !args[i]->as<ASTIdentifier>())
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

    return args;
}

void registerTableFunctionDedup(TableFunctionFactory & factory)
{
    factory.registerFunction("dedup", []() -> TableFunctionPtr { return std::make_shared<TableFunctionDedup>("dedup"); });
}
}
