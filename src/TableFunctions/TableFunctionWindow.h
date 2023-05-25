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

protected:
    virtual void init(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast);
    virtual void handleResultType(const ColumnWithTypeAndName & type_and_name);
    virtual DataTypePtr getElementType(size_t i, const DataTypeTuple *) const = 0;
};
}
}
