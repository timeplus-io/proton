#include "TableFunctionHop.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
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

TableFunctionHop::TableFunctionHop(const String & name_) : TableFunctionHopTumbleBase(name_)
{
    help_message = fmt::format(
        "Table function '{}' requires from 3 to 5 parameters: "
        "<name of the table>, [timestamp column], <hop interval size>, <hop window size>, [time zone]",
        name);
}

void TableFunctionHop::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto streaming_func_ast = func_ast->clone();
    auto * node = streaming_func_ast->as<ASTFunction>();
    assert(node);

    /// hop(table, [timestamp_column], hop_interval, hop_win_interval, [timezone])
    ASTs & args = node->arguments->children;

    if (args.size() < 3)
        throw Exception(help_message, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    if (args.size() > 5)
        throw Exception(help_message, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

    /// First argument is expected to be table
    storage_id= resolveStorageID(args[0], context);

    /// The rest of the arguments are streaming window arguments
    /// Change the name to call the internal streaming window functions
    auto func_name = boost::to_upper_copy(node->name);
    node->name = "__" + func_name;
    node->alias = STREAMING_WINDOW_FUNC_ALIAS;

    /// Prune the arguments to fit the internal hop function
    args.erase(args.begin());

    ASTPtr timestamp_expr_ast;

    if (args.size() == 2)
    {
        /// Assume the first / second arguments are interval, insert `_time`
        args.insert(args.begin(), std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
    }
    else if (args.size() == 3)
    {
        if (args[2]->as<ASTLiteral>())
        {
            /// the last argument is a literal, assume it is a timezone string
            args.insert(args.begin(), std::make_shared<ASTIdentifier>(RESERVED_EVENT_TIME));
        }
        else if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, hop(table, toDateTime32(t), INTERVAL 5 SECOND, ...)
            func_node->alias = STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }
    else
    {
        assert(args.size() == 4);
        if (auto func_node = args[0]->as<ASTFunction>(); func_node)
        {
            /// time column is a transformed one, for example, hop(table, toDateTime32(t), INTERVAL 5 SECOND, ...)
            func_node->alias = STREAMING_TIMESTAMP_ALIAS;
            timestamp_expr_ast = args[0];
        }
    }

    /// Calculate column description
    init(context, std::move(streaming_func_ast), "__HOP(", std::move(timestamp_expr_ast));
}

DataTypePtr TableFunctionHop::getElementType(const DataTypeTuple * tuple) const
{
    DataTypePtr element_type = tuple->getElements()[0];
    assert(isArray(element_type));

    auto array_type = checkAndGetDataType<DataTypeArray>(element_type.get());
    return array_type->getNestedType();
}

void registerTableFunctionHop(TableFunctionFactory & factory)
{
    factory.registerFunction("hop", []() -> TableFunctionPtr { return std::make_shared<TableFunctionHop>("hop"); });
}
}
