#pragma once

#include "TableFunctionProxyBase.h"

namespace DB
{
class TableFunctionHist final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionHist(const String & name_);

private:
    const char * getStorageTypeName() const override { return "hist"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    void init(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
};
}
