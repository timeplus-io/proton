#include "TableFunctionSession.h"

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
    /// session(stream, [timestamp_expr], timeout_interval, [max_emit_interval], [range_comparision])
    doParseArguments(func_ast, context, HOP_HELP_MESSAGE);
}

ASTs TableFunctionSession::checkAndExtractArguments(ASTFunction * node) const
{
    /// session(stream, [timestamp_column], timeout_interval, [max_emit_interval], [range_comparision])
    return checkAndExtractSessionArguments(node);
}

String TableFunctionSession::functionNamePrefix() const
{
    return ProtonConsts::SESSION_FUNC_NAME + "(";
}

DataTypePtr TableFunctionSession::getElementType(const DataTypeTuple * tuple) const
{
    return tuple->getElements()[0];
}

void TableFunctionSession::handleResultType(const ColumnWithTypeAndName & type_and_name)
{
    const auto * tuple_result_type = checkAndGetDataType<DataTypeTuple>(type_and_name.type.get());
    assert(tuple_result_type);
    assert(tuple_result_type->getElements().size() == 2);

    /// If streaming table function is used, we will need project `wstart, wend` columns to metadata
    {
        DataTypePtr element_type = getElementType(tuple_result_type);


        ColumnDescription wstart(ProtonConsts::STREAMING_WINDOW_START, element_type);
        columns.add(wstart);

        ColumnDescription wend(ProtonConsts::STREAMING_WINDOW_END, element_type);
        columns.add(wend);
    }

    {
        DataTypePtr session_start_type = std::make_shared<DataTypeUInt8>();

        ColumnDescription session_start(ProtonConsts::STREAMING_SESSION_START, session_start_type);
        columns.add(session_start);

        DataTypePtr session_end_type = std::make_shared<DataTypeUInt8>();
        ColumnDescription session_end(ProtonConsts::STREAMING_SESSION_END, session_end_type);
        columns.add(session_end);
    }
}

void TableFunctionSession::validateWindow(FunctionDescriptionPtr desc) const
{
    assert(desc && desc->type == WindowType::SESSION);
}

void registerTableFunctionSession(TableFunctionFactory & factory)
{
    factory.registerFunction("session", []() -> TableFunctionPtr { return std::make_shared<TableFunctionSession>("session"); });
}
}
}
