#include "TableFunctionHop.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

TableFunctionHop::TableFunctionHop(const String & name_) : TableFunctionStreamingWindow(name_)
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
    storage_id = resolveStorageID(args[0], context);

    /// The rest of the arguments are streaming window arguments
    /// Change the name to call the internal streaming window functions
    auto func_name = boost::to_upper_copy(node->name);
    node->name = "__" + func_name;
    node->alias = "____SWIN";

    /// Prune the arguments to fit the internal hop function
    node->arguments->children.erase(node->arguments->children.begin());

    /// FIXME here...
    if (node->arguments->children.size() == 2)
    {
        /// assume the first / second arguments are interval, insert `_time`
        node->arguments->children.insert(node->arguments->children.begin(), std::make_shared<ASTIdentifier>("_time"));
    }
    else if (node->arguments->children.size() == 3)
    {
        if (node->arguments->children[2]->as<ASTLiteral>())
        {
            /// the last argument is a literal, assume it is a timezone string
            node->arguments->children.insert(node->arguments->children.begin(), std::make_shared<ASTIdentifier>("_time"));
        }
    }

    /// Calculate column description
    initColumnsDescription(context, streaming_func_ast, "__HOP(");
}

void TableFunctionHop::handleResultType(const ColumnWithTypeAndName & type_and_name)
{
    auto tuple_result_type = checkAndGetDataType<DataTypeTuple>(type_and_name.type.get());
    assert(tuple_result_type);
    assert(tuple_result_type->getElements().size() == 2);

    /// If streaming table function is used, we will need project `wstart, wend` columns to metadata
    DataTypePtr element_type = tuple_result_type->getElements()[0];
    assert(isArray(element_type));

    auto array_type = checkAndGetDataType<DataTypeArray>(element_type.get());
    assert(tuple_result_type);
    element_type = array_type->getNestedType();

    ColumnDescription wstart("wstart", element_type);
    columns.add(wstart);

    ColumnDescription wend("wend", element_type);
    columns.add(wend);
}

void registerTableFunctionHop(TableFunctionFactory & factory)
{
    factory.registerFunction("hop", []() -> TableFunctionPtr { return std::make_shared<TableFunctionHop>("hop"); });
}
}
