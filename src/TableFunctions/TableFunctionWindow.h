#pragma once

#include <TableFunctions/TableFunctionProxyBase.h>

namespace DB
{
class ASTFunction;
class DataTypeTuple;

namespace Streaming
{
class TableFunctionWindow : public TableFunctionProxyBase
{
public:
    explicit TableFunctionWindow(const String & name_) : TableFunctionProxyBase(name_) { }

protected:
    void doParseArguments(const ASTPtr & func_ast, ContextPtr context, const String & help_msg);

    virtual void postArgs(ASTs &) const { }

    virtual ASTs checkAndExtractArguments(ASTFunction *) const = 0;

    NamesAndTypesList getAdditionalResultColumns(const ColumnsWithTypeAndName & arguments) const override;

protected:
    void init(ContextPtr context, ASTPtr streaming_func_ast, ASTPtr timestamp_expr_ast);
    TimestampFunctionDescriptionMutablePtr createTimestampFunctionDescription(ASTPtr timestamp_expr_ast, ContextPtr context);
};
}
}
