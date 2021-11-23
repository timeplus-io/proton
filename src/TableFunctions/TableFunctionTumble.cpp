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

TableFunctionTumble::TableFunctionTumble(const String & name_) : TableFunctionHopTumbleBase(name_)
{
    help_message = fmt::format(
        "Table function '{}' requires from 2 to 4 parameters: "
        "<name of the table>, [timestamp column], <tumble window size>, [time zone]",
        name);
}

void TableFunctionTumble::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto streaming_func_ast = func_ast->clone();
    auto * node = streaming_func_ast->as<ASTFunction>();
    assert(node);

    /// tumble(table, [timestamp_column_expr], win_interval, [timezone])
    ASTs & args = node->arguments->children;

    if (args.size() < 2)
        throw Exception(help_message, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    if (args.size() > 4)
        throw Exception(help_message, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

    /// First argument is expected to be table
    storage_id = resolveStorageID(args[0], context);

    /// The rest of the arguments are streaming window arguments
    /// Change the name to call the internal streaming window functions
    auto func_name = boost::to_upper_copy(node->name);
    node->name = "__" + func_name;
    node->alias = STREAMING_WINDOW_FUNC_ALIAS;

    /// Prune the arguments to fit the internal hop function
    args.erase(args.begin());

    ASTPtr timestamp_expr_ast;

    /// The following logic is adding system default time column to tumble function if user doesn't specify one
    if (args.size() == 1)
    {
        /// Assume the first argument is INTERVAL, and user omits specifying time column. Insert `_time` to use the system default
        args.insert(args.begin(), std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
    }
    else if (args.size() == 2)
    {
        if (args[1]->as<ASTLiteral>())
        {
            /// The last argument is a literal, assume it is a timezone string
            /// User omits specifying time column. Insert `_time` to use the system default
            args.insert(args.begin(), std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
        }
        else if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, tumble(table, toDateTime32(t), INTERVAL 5 SECOND)
            func_node->alias = STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }
    else
    {
        assert(args.size() == 3);
        if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, tumble(table, toDateTime32(t), INTERVAL 5 SECOND)
            func_node->alias = STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }

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
