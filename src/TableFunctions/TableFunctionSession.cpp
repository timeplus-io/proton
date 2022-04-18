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

TableFunctionSession::TableFunctionSession(const String & name_) : TableFunctionProxyBase(name_)
{
}

void TableFunctionSession::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, HOP_HELP_MESSAGE);
}

ASTs TableFunctionSession::checkAndExtractArguments(ASTFunction * node) const
{
    /// session(stream, [timestamp_column], timeout_interval, [key_column1, key_column2, ...])
    return checkAndExtractSessionArguments(node);
}

String TableFunctionSession::functionNamePrefix() const
{
    return SESSION_FUNC_NAME + "(";
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


        ColumnDescription wstart(STREAMING_WINDOW_START, element_type);
        columns.add(wstart);

        ColumnDescription wend(STREAMING_WINDOW_END, element_type);
        columns.add(wend);
    }

    {
        DataTypePtr element_type = std::make_shared<DataTypeUInt32>();

        ColumnDescription session_id(STREAMING_SESSION_ID, std::move(element_type));
        columns.add(session_id);
    }
}

void registerTableFunctionSession(TableFunctionFactory & factory)
{
    factory.registerFunction("session", []() -> TableFunctionPtr { return std::make_shared<TableFunctionSession>("session"); });
}
}
