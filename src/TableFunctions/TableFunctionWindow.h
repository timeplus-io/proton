#pragma once

#include "TableFunctionProxyBase.h"

namespace DB
{
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

    virtual void handleResultType(const ColumnsWithTypeAndName & arguments);

protected:
    void init(ContextPtr context, ASTPtr streaming_func_ast, ASTPtr timestamp_expr_ast);
    TimestampFunctionDescriptionPtr createTimestampFunctionDescription(ASTPtr timestamp_expr_ast, ContextPtr context);
};
}
}
