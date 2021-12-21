#include "TableFunctionTumble.h"

#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

TableFunctionTumble::TableFunctionTumble(const String & name_) : TableFunctionProxyBase(name_)
{
}

void TableFunctionTumble::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto streaming_func_ast = func_ast->clone();
    auto * node = streaming_func_ast->as<ASTFunction>();
    assert(node);

    /// tumble(table, [timestamp_column_expr], win_interval, [timezone])
    auto args = checkAndExtractTumbleArguments(node);

    /// First argument is expected to be table
    storage_id = resolveStorageID(args[0], context);

    /// The rest of the arguments are streaming window arguments
    /// Change the name to call the internal streaming window functions
    auto func_name = boost::to_upper_copy(node->name);
    node->name = "__" + func_name;
    node->alias = STREAMING_WINDOW_FUNC_ALIAS;

    /// Prune the arguments to fit the internal tumble function
    args.erase(args.begin());

    ASTPtr timestamp_expr_ast;

    //// [timestamp_column_expr]
    /// The following logic is adding system default time column to tumble function if user doesn't specify one
    if (args[0])
    {
        if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, tumble(table, toDateTime32(t), INTERVAL 5 SECOND)
            func_node->alias = STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }
    else
        args[0] = std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME);

    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();

    node->arguments->children.swap(args);

    /// Calculate column description
    init(context, std::move(streaming_func_ast), "__TUMBLE(", std::move(timestamp_expr_ast));
}

DataTypePtr TableFunctionTumble::getElementType(const DataTypeTuple * tuple) const
{
    return tuple->getElements()[0];
}

void registerTableFunctionTumble(TableFunctionFactory & factory)
{
    factory.registerFunction("tumble", []() -> TableFunctionPtr { return std::make_shared<TableFunctionTumble>("tumble"); });
}
}
