#include "TableFunctionSession.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace Streaming
{
TableFunctionSession::TableFunctionSession(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionSession::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, SESSION_HELP_MESSAGE);
}

ASTs TableFunctionSession::checkAndExtractArguments(ASTFunction * node) const
{
    /// session(stream, [timestamp_expr], timeout_interval, [max_emit_interval], [range_comparision])
    /// session(stream, [timestamp_expr], timeout_interval, [max_emit_interval], [start_cond, end_cond])
    /// session(stream, [timestamp_expr], timeout_interval, [max_emit_interval], [start_cond, start_with_inclusion, end_cond, end_with_inclusion])
    return checkAndExtractSessionArguments(node);
}

String TableFunctionSession::functionNamePrefix() const
{
    return ProtonConsts::SESSION_FUNC_NAME + "(";
}

void registerTableFunctionSession(TableFunctionFactory & factory)
{
    factory.registerFunction("session", []() -> TableFunctionPtr { return std::make_shared<TableFunctionSession>("session"); });
}
}
}
