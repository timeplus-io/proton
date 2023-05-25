#include "TableFunctionDedup.h"

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

namespace Streaming
{
TableFunctionDedup::TableFunctionDedup(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires at least 2 parameters. The deduplication key column parameters shall not be constant. The `timeout` optional "
        "parameter shall be constant interval seconds like `10s` if present, and the last optional limit parameter shall be integer constant if present. "
        "For example, dedup(test, id, 1s, 1000). dedup(stream, column1[, column2, ..., [timeout, [limit]]])",
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

    streaming_func_desc
        = createStreamingFunctionDescription(dedup_func_ast, std::move(syntax_analyzer_result), context, functionNamePrefix());
}

ASTs TableFunctionDedup::checkAndExtractArguments(ASTFunction * node) const
{
    /// dedup(table, column1, column2, ..., timeout, limit)
    const auto & args = node->arguments->children;
    if (args.size() < 2)
        throw Exception(help_message, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    size_t end_pos = args.size();

    auto check_timeout = [&](size_t pos) {
        /// Check if last argument is interval literal
        if (auto * func = args[pos]->as<ASTFunction>(); func)
        {
            /// When last param or second last is interval param, we requires at least 3 params
            if (args.size() < 3)
                throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

            if (func->name != "to_interval_second")
                throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

            end_pos -= 1;
        }
    };

    auto check_limit = [&](size_t pos) {
        if (auto * lit = args[pos]->as<ASTLiteral>(); lit)
        {
            /// When last param is number limit, we requires at least 4 params
            if (args.size() < 4)
                throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

            if (!isInt64OrUInt64FieldType(lit->value.getType()))
                throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

            end_pos -= 1;
            return true;
        }
        return false;
    };

    if (args.size() > 3)
    {
        if (check_limit(args.size() - 1))
            check_timeout(args.size() - 2);
    }
    else if (args.size() == 3)
    {
        check_timeout(args.size() - 1);
    }

    for (size_t i = 1; i < end_pos; ++i)
        if (!args[i]->as<ASTFunction>() && !args[i]->as<ASTIdentifier>())
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

    return args;
}

String TableFunctionDedup::functionNamePrefix() const
{
    return ProtonConsts::DEDUP_FUNC_NAME + "(";
}

void registerTableFunctionDedup(TableFunctionFactory & factory)
{
    factory.registerFunction("dedup", []() -> TableFunctionPtr { return std::make_shared<TableFunctionDedup>("dedup"); });
}
}
}
