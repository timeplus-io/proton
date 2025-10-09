#include <TableFunctions/TableFunctionDedup.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
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
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
TableFunctionDedup::TableFunctionDedup(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires at least 2 parameters. The deduplication key column parameters shall not be constant. The `timeout` "
        "optional "
        "parameter shall be constant interval seconds like `10s` if present, and the last optional limit parameter shall be integer "
        "constant if present. "
        "For example, dedup(test, id, 1s, 1000). dedup(stream, column1[, column2, ..., [timeout, [limit]]])",
        name);
}

void TableFunctionDedup::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    auto dedup_func_ast = func_ast->clone();
    auto * node = dedup_func_ast->as<ASTFunction>();
    chassert(node);

    auto limit_args{checkAndExtractArguments(node)};

    /// First argument is expected to be table
    resolveStorageID(node->arguments->children[0], context);

    /// Prune the first stream argument
    node->arguments->children.erase(node->arguments->children.begin());

    /// Calculate column description
    TableFunctionProxyBase::calculateColumnDescriptions(context);

    /// Create table func desc
    /// We like to prune `limit_args` constant arguments when evaluate the table expression to make it more performant
    /// since limits args don't need emit to down stream pipeline, we will need discard them anyway after evaluation
    /// if we don't prune them
    streaming_func_desc = createStreamingTableFunctionDescription(dedup_func_ast, context, limit_args);

    /// Project additional result columns of the streaming function to metadata
    for (const auto & column : streaming_func_desc->additional_result_columns)
        columns.add(ColumnDescription{column.name, column.type});
}

/// \return number of limit arguments specified. if `timeout`, and `limit` are both specified, it is 2 for example
size_t TableFunctionDedup::checkAndExtractArguments(ASTFunction * node) const
{
    /// dedup(table, column1, column2, ..., timeout, limit)
    const auto & args = node->arguments->children;
    if (args.size() < 2)
        throw Exception::createRuntime(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, help_message);

    size_t end_pos = args.size();

    auto check_timeout = [&](size_t pos) {
        /// Check if last argument is interval literal
        if (auto * func = args[pos]->as<ASTFunction>(); func)
        {
            /// When last param or second last is interval param, we requires at least 3 params
            if (func->name == "to_interval_second" && args.size() >= 3)
                end_pos -= 1;
        }
    };

    auto check_limit = [&](size_t pos) {
        if (auto * lit = args[pos]->as<ASTLiteral>(); lit)
        {
            if (!isInt64OrUInt64FieldType(lit->value.getType()))
                return false;

            /// When last param is number limit, we requires at least 4 params
            if (args.size() < 4)
                return false;

            end_pos -= 1;
            return true;
        }
        return false;
    };

    if (args.size() > 3)
    {
        if (check_limit(args.size() - 1))
            check_timeout(args.size() - 2);
        else
            check_timeout(args.size() - 1);
    }
    else if (args.size() == 3)
    {
        check_timeout(args.size() - 1);
    }

    for (size_t i = 1; i < end_pos; ++i)
    {
        if (!args[i]->as<ASTFunction>() && !args[i]->as<ASTIdentifier>())
            throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, help_message);
    }

    return args.size() - end_pos;
}

void registerTableFunctionDedup(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "dedup",
        TableFunctionFactoryData{
            []() -> TableFunctionPtr { return std::make_shared<TableFunctionDedup>("dedup"); },
            {},
        },
        TableFunctionFactory::CaseSensitive,
        /*support subquery=*/true);
}
}
}
